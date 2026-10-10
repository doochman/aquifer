import os
import time

import boto3
from botocore.config import Config


def main():
    client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    buckets = ["warehouse", "checkpoints", "quarantine", "artifacts"]
    # Extensions declare extra buckets (comma or space separated) through the environment.
    extra = os.environ.get("EXTRA_BUCKETS", "").replace(",", " ").split()
    buckets += [bucket for bucket in extra if bucket not in buckets]
    deadline = time.monotonic() + 120
    while True:
        try:
            existing = {bucket["Name"] for bucket in client.list_buckets()["Buckets"]}
            for bucket in buckets:
                if bucket not in existing:
                    client.create_bucket(Bucket=bucket)
            for bucket in extra:
                client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


if __name__ == "__main__":
    main()