"""
CloudFormation custom resource: discover which Availability Zones support the
SageMaker Unified Studio MCP endpoint service in the customer's account, and
return two of them for the demo stack's Interface VPC Endpoint subnets.

Why this exists
---------------
AWS randomizes the physical-AZ-to-logical-AZ mapping per account. Hardcoding
subnet AZs like us-east-1a and us-east-1c worked in the author's account, but
in other accounts those logical names can map to physical AZs where SMUS is
not offered, and the stack fails at SMUSVPCEndpoint creation. This Lambda
queries the service at deploy time and returns AZs the account actually
supports.

This module is a source-of-truth copy of the Python that is embedded inline
in the CloudFormation template's ZipFile property. Keep both in sync when
making changes.
"""
import json
import urllib.request
import boto3


def send_cfn_response(event, context, status, data, reason=None):
    """PUT a JSON response to the CloudFormation-provided pre-signed URL."""
    body = json.dumps({
        "Status": status,
        "Reason": reason or f"See CloudWatch Logs: {context.log_stream_name}",
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data,
    }).encode("utf-8")
    req = urllib.request.Request(
        event["ResponseURL"],
        data=body,
        method="PUT",
        headers={"Content-Type": ""},
    )
    urllib.request.urlopen(req, timeout=10)


def lambda_handler(event, context):
    request_type = event["RequestType"]

    # On Delete, there is nothing to undo. Signal success immediately.
    if request_type == "Delete":
        send_cfn_response(event, context, "SUCCESS", {})
        return

    region = context.invoked_function_arn.split(":")[3]
    service_name = f"com.amazonaws.{region}.sagemaker-unified-studio-mcp"

    try:
        ec2 = boto3.client("ec2", region_name=region)
        response = ec2.describe_vpc_endpoint_services(ServiceNames=[service_name])
        details = response.get("ServiceDetails", [])
        if not details:
            raise RuntimeError(
                f"VPC endpoint service {service_name} not found in {region}."
            )
        azs = details[0].get("AvailabilityZones", [])
        if len(azs) < 2:
            raise RuntimeError(
                f"SMUS endpoint service is available in only {len(azs)} AZ(s) "
                f"in this account: {azs}. At least two are required."
            )

        data = {
            "SubnetAZ1": azs[0],
            "SubnetAZ2": azs[1],
            "AllAZs": ",".join(azs),
        }
        print(f"SMUS AZs available: {azs}. Selected {azs[0]} and {azs[1]}.")
        send_cfn_response(event, context, "SUCCESS", data)
    except Exception as exc:  # noqa: BLE001 - must send a response for any failure
        print(f"AZ discovery failed: {exc}")
        # Always respond, otherwise CloudFormation waits an hour before timing out.
        send_cfn_response(event, context, "FAILED", {}, reason=str(exc))
