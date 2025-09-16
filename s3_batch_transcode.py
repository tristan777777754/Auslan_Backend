import os
import subprocess
import boto3
from botocore.exceptions import ClientError

# ---------- Settings ----------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "demo2109bhargav")
S3_PREFIX = os.getenv("S3_PREFIX", "")  # 可以指定 "converted/" 或留空覆蓋

LOCAL_TMP = "./tmp_videos"
os.makedirs(LOCAL_TMP, exist_ok=True)

s3 = boto3.client("s3", region_name=AWS_REGION)

# ---------- Helper: List all mp4 ----------
def list_mp4_objects(bucket, prefix=""):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".mp4"):
                yield obj["Key"]

# ---------- Helper: Transcode with ffmpeg ----------
def transcode_to_h264(in_path, out_path):
    cmd = [
        "ffmpeg", "-y", "-i", in_path,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        out_path
    ]
    subprocess.run(cmd, check=True)

# ---------- Main ----------
def main():
    for key in list_mp4_objects(S3_BUCKET, S3_PREFIX):
        print(f"Processing {key} ...")
        filename = os.path.basename(key)
        local_in = os.path.join(LOCAL_TMP, filename)
        local_out = os.path.join(LOCAL_TMP, "fixed_" + filename)

        # 1. Download
        try:
            s3.download_file(S3_BUCKET, key, local_in)
        except ClientError as e:
            print(f"❌ Failed to download {key}: {e}")
            continue

        # 2. Transcode
        try:
            transcode_to_h264(local_in, local_out)
        except subprocess.CalledProcessError as e:
            print(f"❌ ffmpeg failed for {key}: {e}")
            continue

        # 3. Upload back to S3 (to "converted/" prefix to避免覆蓋)
        new_key = f"converted/{filename}"
        try:
            s3.upload_file(
                local_out, S3_BUCKET, new_key,
                ExtraArgs={"ContentType": "video/mp4"}
            )
            print(f"✅ Uploaded {new_key}")
        except ClientError as e:
            print(f"❌ Failed to upload {new_key}: {e}")

        # 4. Cleanup
        os.remove(local_in)
        os.remove(local_out)

if __name__ == "__main__":
    main()