import boto3

BUCKET_NAME = "demo2109bhargav"

s3 = boto3.client("s3")

def fix_mp4_content_type(bucket):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".mp4"):
                print(f"Checking {key} ...")
                head = s3.head_object(Bucket=bucket, Key=key)
                current_type = head.get("ContentType", "")

                if current_type != "video/mp4":
                    print(f"  -> Fixing {key}, current ContentType: {current_type}")
                    s3.copy_object(
                        Bucket=bucket,
                        CopySource={"Bucket": bucket, "Key": key},
                        Key=key,
                        Metadata=head["Metadata"],  
                        ContentType="video/mp4",
                        MetadataDirective="REPLACE"
                    )
                else:
                    print(f"  -> OK (already video/mp4)")

if __name__ == "__main__":
    fix_mp4_content_type(BUCKET_NAME)