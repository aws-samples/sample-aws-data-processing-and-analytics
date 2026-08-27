# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""amazon-msk-diagnostic MCP server implementation."""

import argparse
import base64
import os
import time
import uuid

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_END,
    Consumer,
    ConsumerGroupTopicPartitions,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    ConfigSource,
    OffsetSpec,
    ResourceType,
)
from loguru import logger
from mcp.server.fastmcp import FastMCP

# Sensitive-data access gate. Enabled at startup via CLI flag or env var.
# Consulted before any tool that returns message payloads runs.
_ALLOW_SENSITIVE_DATA_ACCESS = False


def _sensitive_check(tool_name: str) -> None:
    if not _ALLOW_SENSITIVE_DATA_ACCESS:
        raise RuntimeError(
            f'{tool_name} is disabled because it returns Kafka message payloads, '
            f'which may contain sensitive data. Restart the server with '
            f'--allow-sensitive-data-access=true or set env var '
            f'MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS=true to enable.'
        )


mcp = FastMCP(
    'amazon-msk-diagnostic-mcp',
    instructions=(
        'Use this server to inspect Amazon MSK clusters at the Kafka broker level '
        "using the caller's AWS IAM credentials. Tools require a cluster_arn as their "
        'first argument. Provides read-only broker-direct operations. Does NOT wrap AWS '
        'control-plane APIs (use the AWS MCP Server or aws-api-mcp-server for those) '
        'and does NOT perform writes. If a user references a cluster by name only, '
        'resolve the name to an ARN first (via kafka:ListClusters on the AWS MCP '
        'Server or aws-api-mcp-server), confirm the resolved ARN with the user, and '
        'then pass that ARN to this server.'
    ),
    dependencies=[
        'boto3',
        'confluent-kafka',
        'aws-msk-iam-sasl-signer-python',
    ],
)


def _region_from_arn(cluster_arn: str) -> str:
    # ARN shape: arn:aws:kafka:REGION:ACCOUNT:cluster/NAME/UUID
    parts = cluster_arn.split(':')
    if len(parts) < 4 or parts[0] != 'arn' or parts[2] != 'kafka':
        raise ValueError(f'Not a valid MSK cluster ARN: {cluster_arn}')
    return parts[3]


def _get_bootstrap_brokers(cluster_arn: str, region: str) -> str:
    """Resolve cluster_arn to a bootstrap broker string via the MSK control-plane API.

    Handles both provisioned and serverless clusters. Prefers the public IAM SASL
    endpoint when available (usable from outside the VPC), falling back to the
    private endpoint (usable from inside the VPC / VPN / bastion).
    """
    client = boto3.client('kafka', region_name=region)
    resp = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    brokers = resp.get('BootstrapBrokerStringPublicSaslIam') or resp.get(
        'BootstrapBrokerStringSaslIam'
    )
    if not brokers:
        raise RuntimeError(
            f'Cluster {cluster_arn} does not expose an IAM SASL bootstrap broker string. '
            f'Available keys: {sorted(k for k, v in resp.items() if v)}'
        )
    return brokers


def _oauth_cb(oauth_config, region: str):
    """Callback invoked by librdkafka to refresh the MSK OAUTHBEARER token."""
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    return token, expiry_ms / 1000  # librdkafka wants seconds


def _admin_client_for(cluster_arn: str) -> AdminClient:
    region = _region_from_arn(cluster_arn)
    brokers = _get_bootstrap_brokers(cluster_arn, region)
    return AdminClient(
        {
            'bootstrap.servers': brokers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': lambda cfg: _oauth_cb(cfg, region),
        }
    )


@mcp.tool(name='list_topics')
async def list_topics(cluster_arn: str) -> dict:
    """List all topics on the MSK cluster identified by cluster_arn.

    Uses the caller's AWS credentials (from the SDK provider chain) to authenticate
    to the broker via IAM SASL/OAUTHBEARER. Broker-direct call — reflects current
    cluster state, not the ~1-minute-stale MSK Topic API.

    Args:
        cluster_arn: MSK cluster ARN, e.g.
            arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid-N.
            If the user gave only a cluster name, resolve it to an ARN first
            (e.g., via kafka:ListClusters on the AWS MCP Server or
            aws-api-mcp-server) and confirm the resolved ARN with the user
            before invoking this tool.

    Returns:
        Dict with a `topics` list. Each entry: {name, partition_count, is_internal}.
    """
    admin = _admin_client_for(cluster_arn)
    md = admin.list_topics(timeout=10)
    topics = [
        {
            'name': t.topic,
            'partition_count': len(t.partitions),
            'is_internal': bool(getattr(t, 'is_internal', False)),
        }
        for t in md.topics.values()
    ]
    topics.sort(key=lambda t: t['name'])
    return {'cluster_arn': cluster_arn, 'topic_count': len(topics), 'topics': topics}


@mcp.tool(name='describe_topics')
async def describe_topics(cluster_arn: str, topics: list[str]) -> dict:
    """Describe one or more topics on the MSK cluster, broker-direct.

    Returns per-topic partition layout — for each partition: leader broker id,
    replica broker ids, in-sync replica (ISR) broker ids, and an
    `under_replicated` flag (true when ISR count is below the replica count).
    Common uses: spot under-replicated partitions after a broker restart,
    confirm the leader for a partition before running a targeted producer,
    verify replica placement across brokers.

    Broker-direct: reflects live cluster state, not the ~1-minute-stale MSK
    Topic API. Uses the caller's AWS credentials via IAM SASL/OAUTHBEARER.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first (via kafka:ListClusters on the AWS MCP Server or
            aws-api-mcp-server) and confirm with the user before invoking.
        topics: List of topic names to describe. Must be non-empty. Pass only
            the topics the agent actually needs — describing many topics on a
            large cluster pulls a lot of partition metadata.

    Returns:
        Dict with:
          - cluster_arn
          - summary: {topic_count, requested_count, missing_count,
                      under_replicated_partition_count}
          - topics: list of {name, partition_count, is_internal,
                             under_replicated_partition_count,
                             partitions: [{id, leader, replicas, isr,
                                           under_replicated}]}
          - missing: list of requested topic names that don't exist on the
                     cluster (empty if all found)
    """
    if not topics:
        raise ValueError('topics must be a non-empty list of topic names')

    admin = _admin_client_for(cluster_arn)
    md = admin.list_topics(timeout=10)

    described: list[dict] = []
    missing: list[str] = []
    total_under_replicated = 0

    for name in topics:
        t = md.topics.get(name)
        if t is None or (t.error is not None and not t.partitions):
            missing.append(name)
            continue

        partitions = []
        under_replicated_here = 0
        for pid, p in sorted(t.partitions.items()):
            replicas = list(p.replicas)
            isr = list(p.isrs)
            under = len(isr) < len(replicas)
            if under:
                under_replicated_here += 1
            partitions.append(
                {
                    'id': pid,
                    'leader': p.leader,
                    'replicas': replicas,
                    'isr': isr,
                    'under_replicated': under,
                }
            )

        total_under_replicated += under_replicated_here
        described.append(
            {
                'name': t.topic,
                'partition_count': len(t.partitions),
                'is_internal': bool(getattr(t, 'is_internal', False)),
                'under_replicated_partition_count': under_replicated_here,
                'partitions': partitions,
            }
        )

    described.sort(key=lambda x: x['name'])
    return {
        'cluster_arn': cluster_arn,
        'summary': {
            'topic_count': len(described),
            'requested_count': len(topics),
            'missing_count': len(missing),
            'under_replicated_partition_count': total_under_replicated,
        },
        'topics': described,
        'missing': missing,
    }


_OFFSET_SPEC_ALIASES = {'earliest', 'latest', 'both'}


@mcp.tool(name='get_topic_offsets')
async def get_topic_offsets(cluster_arn: str, topics: list[str], spec: str = 'both') -> dict:
    """Get per-partition earliest / latest offsets for one or more topics.

    For each partition returns the earliest offset (low watermark, oldest
    retained message), the latest offset (high watermark, next message to
    be produced), and the `message_count` between them. Broker-direct,
    reflects live cluster state.

    Common uses: measure how much data a topic currently retains, detect
    partitions receiving no producer traffic (latest offset unchanged
    between two calls), sanity-check whether a topic is empty before
    debugging a consumer, spot skew in message distribution across
    partitions.

    Note: `message_count` counts offset gaps, which include compacted
    tombstones and aborted transactions — not every offset in the range
    corresponds to a live message. Use it as a scale indicator, not an
    exact record count.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first and confirm with the user before invoking.
        topics: List of topic names. Must be non-empty. This is a per-
            partition operation, so listing many high-partition-count topics
            can be expensive — pass only what the agent needs.
        spec: Which offsets to fetch — `"both"` (default) returns both
            earliest and latest per partition and computes `message_count`;
            `"earliest"` returns only the low watermark; `"latest"` returns
            only the high watermark. When `"earliest"` or `"latest"` is
            selected, the other field and `message_count` are omitted.

    Returns:
        Dict with:
          - cluster_arn
          - spec: which specs were fetched ("earliest", "latest", or "both")
          - summary: {topic_count, requested_count, missing_count,
                      partition_count, total_message_count (only for "both")}
          - topics: list of {name, partition_count, total_message_count,
                             partitions: [{id, earliest_offset, latest_offset,
                                           message_count}]}
          - missing: list of requested topic names not on the cluster
    """
    if not topics:
        raise ValueError('topics must be a non-empty list of topic names')
    if spec not in _OFFSET_SPEC_ALIASES:
        raise ValueError(f'spec must be one of {sorted(_OFFSET_SPEC_ALIASES)}, got {spec!r}')

    admin = _admin_client_for(cluster_arn)
    md = admin.list_topics(timeout=10)

    partition_targets: list[tuple[str, int]] = []
    missing: list[str] = []
    for name in topics:
        t = md.topics.get(name)
        if t is None or (t.error is not None and not t.partitions):
            missing.append(name)
            continue
        for pid in sorted(t.partitions.keys()):
            partition_targets.append((name, pid))

    earliest: dict[tuple[str, int], int] = {}
    latest: dict[tuple[str, int], int] = {}

    if partition_targets and spec in ('earliest', 'both'):
        req = {TopicPartition(name, pid): OffsetSpec.earliest() for name, pid in partition_targets}
        for tp, fut in admin.list_offsets(req, request_timeout=10).items():
            try:
                earliest[(tp.topic, tp.partition)] = fut.result().offset
            except Exception as e:
                logger.debug(f'list_offsets(earliest) failed for {tp.topic}[{tp.partition}]: {e}')

    if partition_targets and spec in ('latest', 'both'):
        req = {TopicPartition(name, pid): OffsetSpec.latest() for name, pid in partition_targets}
        for tp, fut in admin.list_offsets(req, request_timeout=10).items():
            try:
                latest[(tp.topic, tp.partition)] = fut.result().offset
            except Exception as e:
                logger.debug(f'list_offsets(latest) failed for {tp.topic}[{tp.partition}]: {e}')

    by_topic: dict[str, list[dict]] = {}
    for name, pid in partition_targets:
        entry: dict = {'id': pid}
        if spec in ('earliest', 'both'):
            entry['earliest_offset'] = earliest.get((name, pid))
        if spec in ('latest', 'both'):
            entry['latest_offset'] = latest.get((name, pid))
        if spec == 'both':
            earliest_off = entry.get('earliest_offset')
            latest_off = entry.get('latest_offset')
            entry['message_count'] = (
                max(0, latest_off - earliest_off)
                if earliest_off is not None and latest_off is not None
                else None
            )
        by_topic.setdefault(name, []).append(entry)

    described: list[dict] = []
    total_message_count = 0
    for name, parts in by_topic.items():
        parts.sort(key=lambda x: x['id'])
        topic_msg_count = None
        if spec == 'both':
            counts = [p['message_count'] for p in parts if p['message_count'] is not None]
            topic_msg_count = sum(counts) if counts else 0
            total_message_count += topic_msg_count
        described.append(
            {
                'name': name,
                'partition_count': len(parts),
                'total_message_count': topic_msg_count,
                'partitions': parts,
            }
        )
    described.sort(key=lambda x: x['name'])

    summary: dict = {
        'topic_count': len(described),
        'requested_count': len(topics),
        'missing_count': len(missing),
        'partition_count': len(partition_targets),
    }
    if spec == 'both':
        summary['total_message_count'] = total_message_count

    return {
        'cluster_arn': cluster_arn,
        'spec': spec,
        'summary': summary,
        'topics': described,
        'missing': missing,
    }


# librdkafka returns config source as a raw int; map to human names.
# ConfigSource is a plain Enum (not IntEnum), so compare via .value.
_CONFIG_SOURCE_NAME = {
    ConfigSource.DYNAMIC_TOPIC_CONFIG.value: 'DYNAMIC_TOPIC_CONFIG',
    ConfigSource.DYNAMIC_BROKER_CONFIG.value: 'DYNAMIC_BROKER_CONFIG',
    ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG.value: 'DYNAMIC_DEFAULT_BROKER_CONFIG',
    ConfigSource.STATIC_BROKER_CONFIG.value: 'STATIC_BROKER_CONFIG',
    ConfigSource.DEFAULT_CONFIG.value: 'DEFAULT_CONFIG',
    ConfigSource.GROUP_CONFIG.value: 'GROUP_CONFIG',
    ConfigSource.UNKNOWN_CONFIG.value: 'UNKNOWN_CONFIG',
}


def _config_source_name(source) -> str:
    return _CONFIG_SOURCE_NAME.get(int(source), str(source))


@mcp.tool(name='list_consumer_groups')
async def list_consumer_groups(cluster_arn: str) -> dict:
    """List all consumer groups on the MSK cluster.

    Returns each group's id, state (STABLE / EMPTY / DEAD / etc.), and whether
    it's a simple (assign-based) or classic (subscribe-based) consumer group.
    Common uses: enumerate groups before drilling into one with
    `describe_consumer_groups`, sanity-check which apps are consuming from the
    cluster, find EMPTY groups that may have been abandoned.

    Broker-direct. Uses the caller's AWS credentials via IAM SASL/OAUTHBEARER.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first (via kafka:ListClusters on the AWS MCP Server or
            aws-api-mcp-server) and confirm with the user before invoking.

    Returns:
        Dict with:
          - cluster_arn
          - summary: {group_count, by_state: {STATE: count, ...}}
          - groups: list of {group_id, state, is_simple_consumer_group}
    """
    admin = _admin_client_for(cluster_arn)
    result = admin.list_consumer_groups(request_timeout=10).result()

    groups = []
    by_state: dict[str, int] = {}
    for g in result.valid:
        state_name = g.state.name if g.state is not None else 'UNKNOWN'
        groups.append(
            {
                'group_id': g.group_id,
                'state': state_name,
                'is_simple_consumer_group': bool(g.is_simple_consumer_group),
            }
        )
        by_state[state_name] = by_state.get(state_name, 0) + 1
    groups.sort(key=lambda x: x['group_id'])

    return {
        'cluster_arn': cluster_arn,
        'summary': {'group_count': len(groups), 'by_state': by_state},
        'groups': groups,
    }


@mcp.tool(name='describe_consumer_groups')
async def describe_consumer_groups(
    cluster_arn: str, groups: list[str], include_offsets: bool = True
) -> dict:
    """Describe one or more consumer groups on the MSK cluster.

    For each requested group returns: state, protocol / partition assignor,
    coordinator broker, members (client id, host, assigned partitions), and —
    when `include_offsets` is True — per-partition committed offset, end
    offset, and lag. Lag = end_offset - committed_offset for each partition
    the group has ever committed to.

    Common uses: diagnose why a consumer group is lagging (which partitions
    are behind, which member owns them), confirm a rebalance has settled
    (state == STABLE with assigned partitions), verify a group has consumed
    up to a known offset.

    Broker-direct. Uses the caller's AWS credentials via IAM SASL/OAUTHBEARER.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first (via kafka:ListClusters on the AWS MCP Server or
            aws-api-mcp-server) and confirm with the user before invoking.
        groups: List of consumer group ids to describe. Must be non-empty.
        include_offsets: When True (default), also fetch committed offsets
            and current end offsets to compute lag per partition. Set False
            to skip the extra broker round-trips when only group metadata is
            needed (e.g. members, coordinator).

    Returns:
        Dict with:
          - cluster_arn
          - summary: {group_count, requested_count, missing_count,
                      total_lag, by_state: {STATE: count, ...}}
          - groups: list of per-group details including
              {group_id, state, partition_assignor, coordinator,
               member_count, members: [...],
               total_lag, offsets: [{topic, partition, committed_offset,
                                     end_offset, lag}]}
          - missing: list of requested group ids that don't exist or failed
                     to describe (empty if all found)
    """
    if not groups:
        raise ValueError('groups must be a non-empty list of consumer group ids')

    admin = _admin_client_for(cluster_arn)

    described: list[dict] = []
    missing: list[str] = []
    total_lag = 0
    by_state: dict[str, int] = {}

    describe_futures = admin.describe_consumer_groups(
        list(groups), include_authorized_operations=False, request_timeout=10
    )

    for gid, fut in describe_futures.items():
        try:
            desc = fut.result()
        except Exception as e:
            missing.append(gid)
            logger.debug(f'describe_consumer_groups failed for {gid}: {e}')
            continue

        state_name = desc.state.name if desc.state is not None else 'UNKNOWN'
        by_state[state_name] = by_state.get(state_name, 0) + 1

        coordinator = None
        if desc.coordinator is not None:
            coordinator = {
                'id': desc.coordinator.id,
                'host': desc.coordinator.host,
                'port': desc.coordinator.port,
            }

        members = []
        for m in desc.members:
            assignment_partitions = []
            if m.assignment is not None:
                for tp in m.assignment.topic_partitions:
                    assignment_partitions.append({'topic': tp.topic, 'partition': tp.partition})
            members.append(
                {
                    'member_id': m.member_id,
                    'client_id': m.client_id,
                    'host': m.host,
                    'group_instance_id': getattr(m, 'group_instance_id', None),
                    'assigned_partitions': assignment_partitions,
                }
            )

        group_entry: dict = {
            'group_id': gid,
            'state': state_name,
            'is_simple_consumer_group': bool(desc.is_simple_consumer_group),
            'partition_assignor': desc.partition_assignor or None,
            'coordinator': coordinator,
            'member_count': len(members),
            'members': members,
        }

        if include_offsets:
            offsets_entries, group_lag = _consumer_group_offsets_with_lag(admin, gid)
            group_entry['total_lag'] = group_lag
            group_entry['offsets'] = offsets_entries
            total_lag += group_lag

        described.append(group_entry)

    described.sort(key=lambda x: x['group_id'])
    return {
        'cluster_arn': cluster_arn,
        'summary': {
            'group_count': len(described),
            'requested_count': len(groups),
            'missing_count': len(missing),
            'total_lag': total_lag if include_offsets else None,
            'by_state': by_state,
        },
        'groups': described,
        'missing': missing,
    }


def _consumer_group_offsets_with_lag(admin: AdminClient, group_id: str) -> tuple[list[dict], int]:
    """Fetch a group's committed offsets and compute lag against the current end.

    Returns (offsets_list, total_lag). offsets_list entries look like
    {topic, partition, committed_offset, end_offset, lag}. `end_offset` and
    `lag` are null when the end offset lookup fails for a partition.
    """
    req = ConsumerGroupTopicPartitions(group_id=group_id)
    committed_result = admin.list_consumer_group_offsets([req], request_timeout=10)[
        group_id
    ].result()

    if not committed_result.topic_partitions:
        return [], 0

    end_offset_request = {
        TopicPartition(tp.topic, tp.partition): OffsetSpec.latest()
        for tp in committed_result.topic_partitions
    }
    end_offset_futures = admin.list_offsets(end_offset_request, request_timeout=10)
    end_offsets: dict[tuple[str, int], int] = {}
    for tp, fut in end_offset_futures.items():
        try:
            end_offsets[(tp.topic, tp.partition)] = fut.result().offset
        except Exception as e:
            logger.debug(f'list_offsets failed for {tp.topic}[{tp.partition}]: {e}')

    offsets = []
    total_lag = 0
    for tp in committed_result.topic_partitions:
        end = end_offsets.get((tp.topic, tp.partition))
        committed = tp.offset if tp.offset >= 0 else None
        if end is not None and committed is not None:
            lag = max(0, end - committed)
            total_lag += lag
        else:
            lag = None
        offsets.append(
            {
                'topic': tp.topic,
                'partition': tp.partition,
                'committed_offset': committed,
                'end_offset': end,
                'lag': lag,
            }
        )
    offsets.sort(key=lambda x: (x['topic'], x['partition']))
    return offsets, total_lag


# The genuinely useful overrides — values that make this resource behave
# differently from the shipped defaults. Excludes DEFAULT_CONFIG (the cluster
# default) and STATIC_BROKER_CONFIG (values from the broker's static
# properties file at boot — huge and mostly noise).
_INTERESTING_CONFIG_SOURCES = frozenset(
    {
        ConfigSource.DYNAMIC_TOPIC_CONFIG.value,
        ConfigSource.DYNAMIC_BROKER_CONFIG.value,
        ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG.value,
        ConfigSource.GROUP_CONFIG.value,
    }
)


def _non_default_config_entries(config_dict) -> list[dict]:
    """Return only entries whose source is a dynamic override, sorted by name.

    librdkafka returns entry.source as a raw int, so we compare on int value.
    Sensitive values are returned with value=null.
    """
    overrides = []
    for name, entry in config_dict.items():
        if int(entry.source) not in _INTERESTING_CONFIG_SOURCES:
            continue
        overrides.append(
            {
                'name': name,
                'value': None if entry.is_sensitive else entry.value,
                'source': _config_source_name(entry.source),
                'is_read_only': bool(entry.is_read_only),
                'is_sensitive': bool(entry.is_sensitive),
            }
        )
    overrides.sort(key=lambda x: x['name'])
    return overrides


@mcp.tool(name='describe_topic_configs')
async def describe_topic_configs(cluster_arn: str, topics: list[str]) -> dict:
    """Describe dynamic configuration overrides on one or more topics.

    Returns only entries with a dynamic source (`DYNAMIC_TOPIC_CONFIG`,
    `GROUP_CONFIG`) — i.e. the values that were explicitly set on the topic,
    not inherited defaults or broker static values. Each entry includes
    name, value, source, and read-only / sensitive flags. Sensitive values
    are redacted (value=null). An empty `overrides` list means the topic is
    running on pure cluster defaults.

    Common uses: catch topics with a non-default compression codec, retention
    override, min.insync.replicas mismatch, cleanup policy set to compact.
    Broker-direct, so this catches overrides applied via `kafka-configs.sh`
    that never went through the MSK Topic API.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first and confirm with the user before invoking.
        topics: List of topic names. Must be non-empty.

    Returns:
        Dict with:
          - cluster_arn
          - summary: {topic_count, requested_count, missing_count,
                      total_override_count}
          - topics: list of {name, override_count, overrides: [...]}
          - missing: list of requested topic names that couldn't be described.
    """
    if not topics:
        raise ValueError('topics must be a non-empty list of topic names')

    admin = _admin_client_for(cluster_arn)
    resources = [ConfigResource(ResourceType.TOPIC, name) for name in topics]
    futures = admin.describe_configs(resources, request_timeout=10)

    described: list[dict] = []
    missing: list[str] = []
    total_overrides = 0
    for res, fut in futures.items():
        try:
            cfg = fut.result()
        except Exception as e:
            missing.append(res.name)
            logger.debug(f'describe_configs failed for topic {res.name}: {e}')
            continue

        overrides = _non_default_config_entries(cfg)
        total_overrides += len(overrides)
        described.append(
            {
                'name': res.name,
                'override_count': len(overrides),
                'overrides': overrides,
            }
        )

    described.sort(key=lambda x: x['name'])
    return {
        'cluster_arn': cluster_arn,
        'summary': {
            'topic_count': len(described),
            'requested_count': len(topics),
            'missing_count': len(missing),
            'total_override_count': total_overrides,
        },
        'topics': described,
        'missing': missing,
    }


@mcp.tool(name='describe_broker_configs')
async def describe_broker_configs(cluster_arn: str, broker_ids: list[int]) -> dict:
    """Describe dynamic configuration overrides on one or more brokers.

    Returns only entries with a dynamic source (`DYNAMIC_BROKER_CONFIG`,
    `DYNAMIC_DEFAULT_BROKER_CONFIG`) — the values changed post-boot, not
    the static properties file the broker started with. Sensitive values
    are redacted. An empty `overrides` list means the broker is running on
    the properties file only (no dynamic overrides applied).

    Common uses: confirm a cluster-wide dynamic config was applied to every
    broker; catch drift between brokers; verify a live `kafka-configs.sh
    --alter` took effect.

    Broker-direct. Confluent's `describe_configs` API only allows ONE broker
    resource per underlying request, so this tool makes one call per broker
    id sequentially — keep the id list to what you actually need.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first and confirm with the user before invoking.
        broker_ids: List of broker ids (integers, e.g. [1, 2, 3]). Must be
            non-empty.

    Returns:
        Dict with:
          - cluster_arn
          - summary: {broker_count, requested_count, missing_count,
                      total_override_count}
          - brokers: list of {broker_id, override_count, overrides: [...]}
          - missing: list of requested broker ids that couldn't be described.
    """
    if not broker_ids:
        raise ValueError('broker_ids must be a non-empty list of broker ids (integers)')

    admin = _admin_client_for(cluster_arn)

    described: list[dict] = []
    missing: list[int] = []
    total_overrides = 0

    for bid in broker_ids:
        resource = ConfigResource(ResourceType.BROKER, str(bid))
        try:
            cfg = admin.describe_configs([resource], request_timeout=10)[resource].result()
        except Exception as e:
            missing.append(bid)
            logger.debug(f'describe_configs failed for broker {bid}: {e}')
            continue

        overrides = _non_default_config_entries(cfg)
        total_overrides += len(overrides)
        described.append(
            {
                'broker_id': bid,
                'override_count': len(overrides),
                'overrides': overrides,
            }
        )

    described.sort(key=lambda x: x['broker_id'])
    return {
        'cluster_arn': cluster_arn,
        'summary': {
            'broker_count': len(described),
            'requested_count': len(broker_ids),
            'missing_count': len(missing),
            'total_override_count': total_overrides,
        },
        'brokers': described,
        'missing': missing,
    }


_STARTING_OFFSET_KEYS = {
    'position',
    'timestamp_ms',
    'offset',
    'consumer_group',
}


def _encode_bytes(b) -> tuple:
    """Return (value, encoding). Try utf-8, fall back to base64."""
    if b is None:
        return None, None
    try:
        return b.decode('utf-8'), 'utf-8'
    except (UnicodeDecodeError, AttributeError):
        return base64.b64encode(b).decode('ascii'), 'base64'


def _resolve_starting_offsets(
    admin: AdminClient,
    topic: str,
    partitions: list,
    starting_offsets: dict,
) -> list:
    """Return a list of TopicPartition with .offset populated for the requested start.

    Raises ValueError on malformed input.
    """
    keys_present = set(starting_offsets.keys()) & _STARTING_OFFSET_KEYS
    if len(keys_present) != 1:
        raise ValueError(
            f'starting_offsets must have exactly one of {sorted(_STARTING_OFFSET_KEYS)}; '
            f'got keys {sorted(starting_offsets.keys())}'
        )
    key = next(iter(keys_present))

    if key == 'position':
        pos = starting_offsets['position']
        if pos == 'earliest':
            return [TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions]
        if pos == 'latest':
            return [TopicPartition(topic, p, OFFSET_END) for p in partitions]
        raise ValueError(f"position must be 'earliest' or 'latest'; got {pos!r}")

    if key == 'timestamp_ms':
        ts = int(starting_offsets['timestamp_ms'])
        spec = OffsetSpec.for_timestamp(ts)
        req = {TopicPartition(topic, p): spec for p in partitions}
        futures = admin.list_offsets(req, request_timeout=10)
        resolved = []
        for tp, fut in futures.items():
            try:
                offset = fut.result().offset
            except Exception as e:
                logger.debug(f'list_offsets(timestamp) failed for {tp}: {e}')
                offset = OFFSET_END  # no message at/after ts; start at tail
            if offset < 0:
                offset = OFFSET_END
            resolved.append(TopicPartition(topic, tp.partition, offset))
        return resolved

    if key == 'offset':
        offset = int(starting_offsets['offset'])
        target_partition = starting_offsets.get('partition')
        if target_partition is None:
            raise ValueError(
                "starting_offsets 'offset' form requires 'partition' too, "
                "e.g. {'offset': 12345, 'partition': 0}"
            )
        target_partition = int(target_partition)
        if target_partition not in partitions:
            raise ValueError(
                f'partition {target_partition} not in requested partitions {partitions}'
            )
        return [TopicPartition(topic, target_partition, offset)]

    if key == 'consumer_group':
        gid = str(starting_offsets['consumer_group'])
        req = ConsumerGroupTopicPartitions(group_id=gid)
        committed = admin.list_consumer_group_offsets([req], request_timeout=10)[gid].result()
        by_partition = {
            tp.partition: tp.offset
            for tp in committed.topic_partitions
            if tp.topic == topic and tp.offset >= 0
        }
        resolved = []
        for p in partitions:
            resolved.append(TopicPartition(topic, p, by_partition.get(p, OFFSET_BEGINNING)))
        return resolved

    raise ValueError(f'unhandled starting_offsets key: {key}')


@mcp.tool(name='read_topic_data')
async def read_topic_data(
    cluster_arn: str,
    topic: str,
    starting_offsets: dict,
    partitions: list[int] | None = None,
    max_messages: int = 100,
    max_bytes: int = 1_000_000,
    timeout_seconds: int = 10,
) -> dict:
    """Read message payloads from a topic. SENSITIVE — disabled by default.

    Returns actual message contents. Because message payloads can contain
    PII, credentials, PHI, or otherwise sensitive data, this tool is
    disabled unless the server operator explicitly opted in at startup with
    `--allow-sensitive-data-access=true` (or env var
    `MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS=true`). If the flag is off, this
    tool raises immediately without contacting the broker.

    Read-only: uses a random one-shot consumer group with auto-commit and
    auto-offset-store both disabled, and assigns partitions manually. It
    does NOT interfere with any existing consumer group's committed offsets.

    Bytes are decoded as UTF-8 when possible; otherwise base64-encoded with
    `key_encoding` / `value_encoding` flagged as `"base64"`. The response
    is bounded by both `max_messages` and `max_bytes` — whichever hits
    first. `summary.truncated=true` indicates a limit was reached.

    Args:
        cluster_arn: MSK cluster ARN. Resolve any cluster-name-only reference
            to an ARN first (via kafka:ListClusters on the AWS MCP Server or
            aws-api-mcp-server) and confirm with the user before invoking.
        topic: Single topic name to read from.
        starting_offsets: Dict specifying where to start. Provide exactly ONE
            of these forms:
              - {"position": "earliest"} — start at the beginning of each
                partition (respecting retention).
              - {"position": "latest"} — start at the tail. Usually returns
                zero messages unless producer traffic arrives during the
                timeout window.
              - {"timestamp_ms": 1755000000000} — start at the first offset
                whose record timestamp is at or after the given epoch ms.
              - {"offset": 12345, "partition": 0} — start at a specific
                (partition, offset) pair. `partitions` (below) is ignored.
              - {"consumer_group": "my-group"} — start from wherever the
                given consumer group has committed for this topic. Never
                affects the group's offsets.
        partitions: Optional list of partition ids to restrict the read to.
            Defaults to all partitions of the topic. Ignored for the
            single-partition `{"offset", "partition"}` form.
        max_messages: Cap on the number of messages returned. Default 100.
        max_bytes: Cap on cumulative (key + value) bytes, applied before
            base64 encoding. Default ~1 MB.
        timeout_seconds: Total wall-clock budget for the read. Reads at
            `latest` on an idle topic will simply return empty after this
            elapses.

    Returns:
        Dict with:
          - cluster_arn, topic
          - summary: {message_count, byte_count, partition_count, truncated}
          - messages: list of {partition, offset, timestamp_ms,
              timestamp_type, key, key_encoding, value, value_encoding,
              value_size_bytes, headers}
    """
    _sensitive_check('read_topic_data')

    if not topic:
        raise ValueError('topic must be a non-empty string')
    if max_messages <= 0:
        raise ValueError('max_messages must be positive')
    if max_bytes <= 0:
        raise ValueError('max_bytes must be positive')
    if timeout_seconds <= 0:
        raise ValueError('timeout_seconds must be positive')

    admin = _admin_client_for(cluster_arn)
    # Passing a topic name to list_topics() returns a phantom entry for missing
    # topics on some brokers, so list all and check membership.
    md = admin.list_topics(timeout=10)
    topic_md = md.topics.get(topic)
    if topic_md is None or (topic_md.error is not None and not topic_md.partitions):
        raise ValueError(f'topic {topic!r} not found on cluster')

    all_partition_ids = sorted(topic_md.partitions.keys())
    requested = partitions if partitions else all_partition_ids
    for p in requested:
        if p not in topic_md.partitions:
            raise ValueError(
                f'partition {p} not found on topic {topic!r} (available: {all_partition_ids})'
            )

    start_tps = _resolve_starting_offsets(admin, topic, requested, starting_offsets)

    region = _region_from_arn(cluster_arn)
    brokers = _get_bootstrap_brokers(cluster_arn, region)
    consumer = Consumer(
        {
            'bootstrap.servers': brokers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': lambda cfg: _oauth_cb(cfg, region),
            'group.id': f'msk-mcp-read-{uuid.uuid4().hex[:12]}',
            'enable.auto.commit': False,
            'enable.auto.offset.store': False,
            'auto.offset.reset': 'error',
            'client.id': 'amazon-msk-diagnostic-mcp/read_topic_data',
        }
    )
    consumer.assign(start_tps)

    messages: list[dict] = []
    partitions_hit: set = set()
    byte_count = 0
    truncated = False
    deadline = time.time() + timeout_seconds
    poll_budget = min(timeout_seconds, 1.0)

    try:
        while len(messages) < max_messages and byte_count < max_bytes:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            msg = consumer.poll(timeout=min(poll_budget, remaining))
            if msg is None:
                continue
            if msg.error():
                logger.debug(f'consumer poll returned error: {msg.error()}')
                continue

            key_bytes = msg.key()
            val_bytes = msg.value()
            key_size = len(key_bytes) if key_bytes else 0
            val_size = len(val_bytes) if val_bytes else 0
            if byte_count + key_size + val_size > max_bytes and messages:
                truncated = True
                break
            byte_count += key_size + val_size

            key_str, key_enc = _encode_bytes(key_bytes)
            val_str, val_enc = _encode_bytes(val_bytes)

            ts_type, ts_val = msg.timestamp()
            ts_type_name = {
                0: 'NOT_AVAILABLE',
                1: 'CREATE_TIME',
                2: 'LOG_APPEND_TIME',
            }.get(ts_type, str(ts_type))

            headers_list = []
            raw_headers = msg.headers() or []
            for hk, hv in raw_headers:
                hv_str, hv_enc = _encode_bytes(hv)
                headers_list.append({'key': hk, 'value': hv_str, 'encoding': hv_enc})

            messages.append(
                {
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp_ms': ts_val if ts_type != 0 else None,
                    'timestamp_type': ts_type_name,
                    'key': key_str,
                    'key_encoding': key_enc,
                    'value': val_str,
                    'value_encoding': val_enc,
                    'value_size_bytes': val_size,
                    'headers': headers_list,
                }
            )
            partitions_hit.add(msg.partition())

        if len(messages) >= max_messages:
            truncated = True
    finally:
        consumer.close()

    return {
        'cluster_arn': cluster_arn,
        'topic': topic,
        'summary': {
            'message_count': len(messages),
            'byte_count': byte_count,
            'partition_count': len(partitions_hit),
            'truncated': truncated,
        },
        'messages': messages,
    }


def main():
    """Run the MCP server."""
    global _ALLOW_SENSITIVE_DATA_ACCESS

    parser = argparse.ArgumentParser(
        prog='amazon-msk-diagnostic-mcp',
        description='Amazon MSK broker-level diagnostic MCP server.',
    )
    parser.add_argument(
        '--allow-sensitive-data-access',
        nargs='?',
        const='true',
        default=None,
        help='Enable tools that return message payloads (read_topic_data). '
        'Off by default. Same effect as MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS=true.',
    )
    args = parser.parse_args()

    env_flag = os.environ.get('MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS', '').lower()
    cli_flag = (args.allow_sensitive_data_access or '').lower()
    _ALLOW_SENSITIVE_DATA_ACCESS = env_flag in ('true', '1', 'yes') or cli_flag in (
        'true',
        '1',
        'yes',
    )

    logger.info(
        f'Starting amazon-msk-diagnostic-mcp (sensitive_data_access={_ALLOW_SENSITIVE_DATA_ACCESS})'
    )
    mcp.run()


if __name__ == '__main__':
    main()
