# main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Literal, List
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions,
)
import boto3
from datetime import datetime, timedelta
from pydub import AudioSegment
import tempfile
import os

# 1. Load .env into environment
from dotenv import load_dotenv
load_dotenv()  # make sure python-dotenv is installed

app = FastAPI()

class ExtractRequest(BaseModel):
    source_type: Literal["azure", "s3"]
    container_or_bucket: str
    prefix: str
    expiry_days: int = 7

class FileRecord(BaseModel):
    file_name: str
    path: str
    url: str
    duration_seconds: float | None

@app.post("/extract-audio-urls", response_model=List[FileRecord])
async def extract_audio_urls(req: ExtractRequest):
    results: List[FileRecord] = []
    expiry = datetime.utcnow() + timedelta(days=req.expiry_days)

    if req.source_type == "azure":
        # --- Azure setup ----------------------------------
        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        account_key = os.getenv("AZURE_ACCOUNT_KEY")
        print("Loaded CONN_STR:", repr(conn_str))
        print("Loaded ACCOUNT_KEY:", repr(account_key))
        if not conn_str or not account_key:
            raise HTTPException(500, "Azure credentials not set")

        svc = BlobServiceClient.from_connection_string(conn_str)
        container = svc.get_container_client(req.container_or_bucket)

        # List blobs and generate SAS URLs
        for blob in container.list_blobs(name_starts_with=req.prefix):
            name = blob.name
            if not name.lower().endswith((".wav", ".mp3", ".m4a")):
                continue

            sas = generate_blob_sas(
                account_name  = svc.account_name,
                account_key   = account_key,           # must match connection string
                container_name= req.container_or_bucket,
                blob_name     = name,
                permission    = BlobSasPermissions(read=True),
                expiry        = expiry,
            )

            url = (
                f"https://{svc.account_name}"
                f".blob.core.windows.net/{req.container_or_bucket}/{name}?{sas}"
            )

            # Download locally and measure duration
            tmp_dir = tempfile.mkdtemp()
            local_path = os.path.join(tmp_dir, os.path.basename(name))
            with open(local_path, "wb") as f:
                f.write(container.get_blob_client(name).download_blob().readall())

            try:
                audio = AudioSegment.from_file(local_path)
                duration = audio.duration_seconds
            except Exception:
                duration = None

            results.append(
                FileRecord(
                    file_name=os.path.basename(name),
                    path=name,
                    url=url,
                    duration_seconds=duration,
                )
            )

    elif req.source_type == "s3":
        # --- AWS S3 setup --------------------------------
        aws_access = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

        if not aws_access or not aws_secret:
            raise HTTPException(
                status_code=500,
                detail=(
                    "AWS credentials not set. "
                    "Please define AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
                )
            )

        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access,
            aws_secret_access_key=aws_secret,
        )

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=req.container_or_bucket, Prefix=req.prefix
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.lower().endswith((".wav", ".mp3", ".m4a")):
                    continue

                url = s3.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": req.container_or_bucket, "Key": key},
                    ExpiresIn=req.expiry_days * 24 * 3600,
                )

                tmp_dir = tempfile.mkdtemp()
                local_path = os.path.join(tmp_dir, os.path.basename(key))
                s3.download_file(req.container_or_bucket, key, local_path)

                try:
                    audio = AudioSegment.from_file(local_path)
                    duration = audio.duration_seconds
                except Exception:
                    duration = None

                results.append(
                    FileRecord(
                        file_name=os.path.basename(key),
                        path=key,
                        url=url,
                        duration_seconds=duration,
                    )
                )

    else:
        raise HTTPException(status_code=400, detail="Invalid source_type")

    return results
