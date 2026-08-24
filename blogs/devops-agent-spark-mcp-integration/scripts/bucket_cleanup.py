"""
CloudFormation custom resource: empty the demo S3 bucket on stack delete so
that CloudFormation can then delete the bucket itself. AWS CloudFormation
refuses to delete a non-empty bucket, and this template's bucket accumulates
Spark event logs and driver/executor stdout/stderr during the demo — plus
whatever the reader has manually uploaded (PySpark script, Parquet input).

Without this Lambda, the reader has to run
`aws s3 rm s3://<demo-bucket> --recursive` before `cloudformation delete-stack`.
With it, the single delete-stack call is enough.

Behavior
--------
* Create / Update: no-op. Returns SUCCESS immediately.
* Delete: lists every current object AND every non-current version in the
  bucket, deletes them in batches of 1000 (the S3 API limit), then returns
  SUCCESS so CloudFormation can proceed with the bucket delete.

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


def empty_bucket(bucket_name):
    """List and delete every object + version in the bucket. Idempotent."""
    s3 = boto3.client("s3")
    total_deleted = 0

    # Current object versions
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket_name):
        to_delete = []
        for obj in page.get("Versions", []) + page.get("DeleteMarkers", []):
            to_delete.append({"Key": obj["Key"], "VersionId": obj["VersionId"]})
        # S3 delete_objects accepts up to 1000 keys per call.
        while to_delete:
            batch, to_delete = to_delete[:1000], to_delete[1000:]
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
            total_deleted += len(batch)

    # Fallback for un-versioned buckets: list_object_versions may return
    # empty. Also drain any remaining objects via list_objects_v2.
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        to_delete = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        while to_delete:
            batch, to_delete = to_delete[:1000], to_delete[1000:]
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
            total_deleted += len(batch)

    return total_deleted


def lambda_handler(event, context):
    request_type = event["RequestType"]
    props = event.get("ResourceProperties", {})
    bucket_name = props.get("BucketName")

    if request_type in ("Create", "Update"):
        # Nothing to do on create/update — the bucket cleanup only runs on delete.
        send_cfn_response(event, context, "SUCCESS", {"BucketName": bucket_name or ""})
        return

    # request_type == "Delete"
    try:
        if not bucket_name:
            # Older stack update / drift may have removed the property. Nothing
            # to do; do not fail the delete.
            print("No BucketName in ResourceProperties; skipping empty.")
            send_cfn_response(event, context, "SUCCESS", {})
            return

        print(f"Emptying s3://{bucket_name} …")
        deleted = empty_bucket(bucket_name)
        print(f"Deleted {deleted} object(s) / version(s) from {bucket_name}.")
        send_cfn_response(event, context, "SUCCESS", {"Deleted": str(deleted)})
    except boto3.exceptions.botocore.exceptions.ClientError as exc:  # type: ignore[attr-defined]
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchBucket", "404"):
            # Bucket already gone (e.g. manually deleted). Treat as success.
            print(f"Bucket {bucket_name} does not exist. Nothing to empty.")
            send_cfn_response(event, context, "SUCCESS", {})
            return
        # Any other AWS error: report and fail so the operator can see it.
        print(f"Bucket empty failed: {exc}")
        send_cfn_response(event, context, "FAILED", {}, reason=str(exc))
    except Exception as exc:  # noqa: BLE001 - always respond to CFN
        print(f"Bucket empty failed: {exc}")
        send_cfn_response(event, context, "FAILED", {}, reason=str(exc))
