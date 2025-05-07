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
import os
from dotenv import load_dotenv

load_dotenv()  # loads AZURE_* and AWS_* from your .env

app = FastAPI()

class ExtractRequest(BaseModel):
    source_type: Literal["azure", "s3"]
    container_or_bucket: str
    prefix: str
    expiry_days: int = 7

class FileRecord(BaseModel):
    path: str
    url: str

@app.post("/extract-audio-urls", response_model=List[FileRecord])
async def extract_audio_urls(req: ExtractRequest):
    results: List[FileRecord] = []
    expiry = datetime.utcnow() + timedelta(days=req.expiry_days)

    if req.source_type == "azure":
        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        account_key = os.getenv("AZURE_ACCOUNT_KEY")
        if not conn_str or not account_key:
            raise HTTPException(500, "Azure credentials not set")

        svc = BlobServiceClient.from_connection_string(conn_str)
        container = svc.get_container_client(req.container_or_bucket)

        for blob in container.list_blobs(name_starts_with=req.prefix):
            name = blob.name
            if not name.lower().endswith((".wav", ".mp3", ".m4a")):
                continue

            sas = generate_blob_sas(
                account_name   = svc.account_name,
                account_key    = account_key,
                container_name = req.container_or_bucket,
                blob_name      = name,
                permission     = BlobSasPermissions(read=True),
                expiry         = expiry,
            )
            url = (
                f"https://{svc.account_name}"
                f".blob.core.windows.net/{req.container_or_bucket}/{name}?{sas}"
            )
            results.append(FileRecord(path=name, url=url))

    elif req.source_type == "s3":
        aws_access = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not aws_access or not aws_secret:
            raise HTTPException(500, "AWS credentials not set")

        s3 = boto3.client(
            "s3",
            aws_access_key_id     = aws_access,
            aws_secret_access_key = aws_secret,
        )
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=req.container_or_bucket, Prefix=req.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.lower().endswith((".wav", ".mp3", ".m4a")):
                    continue

                url = s3.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": req.container_or_bucket, "Key": key},
                    ExpiresIn=req.expiry_days * 24 * 3600,
                )
                results.append(FileRecord(path=key, url=url))

    else:
        raise HTTPException(400, "Invalid source_type")

    return results
