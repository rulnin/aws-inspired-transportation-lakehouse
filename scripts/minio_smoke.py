"""Write and read a temporary object through the local MinIO S3 endpoint."""

from __future__ import annotations

import os
import time
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import boto3
from botocore.config import Config


def main() -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", "http://127.0.0.1:9000")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen(f"{endpoint}/minio/health/ready", timeout=3):
                break
        except URLError:
            if time.monotonic() >= deadline:
                raise TimeoutError("MinIO did not become ready within 60 seconds") from None
            time.sleep(2)

    existing_buckets = {item["Name"] for item in client.list_buckets()["Buckets"]}
    if bucket not in existing_buckets:
        client.create_bucket(Bucket=bucket)

    key = f"infra-smoke/{uuid.uuid4().hex}.txt"
    expected = b"MinIO S3-compatible object round trip passed."
    client.put_object(Bucket=bucket, Key=key, Body=expected)
    response = client.get_object(Bucket=bucket, Key=key)
    actual = response["Body"].read()
    response["Body"].close()
    client.delete_object(Bucket=bucket, Key=key)

    if actual != expected:
        raise RuntimeError("MinIO returned different bytes from those uploaded")
    print(f"MinIO object round trip passed: s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
