from minio import Minio
from minio.error import S3Error
import logging
from pathlib import Path
from typing import Optional
import io
import pandas as pd


class MinIOConnector:
    def __init__(self, config):
        self.client = Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_SECURE
        )
        self.bucket = config.MINIO_BUCKET
        self.logger = logging.getLogger('minio')

    def ensure_bucket_exists(self):
        """确保存储桶存在"""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                self.logger.info(f"Created bucket: {self.bucket}")
        except S3Error as e:
            self.logger.error(f"Bucket operation failed: {str(e)}")
            raise

    def upload_file(self, file_path: Path, object_name: Optional[str] = None):
        """上传文件到MinIO"""
        object_name = object_name or file_path.name
        try:
            self.client.fput_object(
                self.bucket,
                object_name,
                str(file_path)
            )
            self.logger.info(f"Uploaded {file_path} to {self.bucket}/{object_name}")
        except S3Error as e:
            self.logger.error(f"Upload failed: {str(e)}")
            raise

    def upload_dataframe(self, df: pd.DataFrame, object_name: str):
        """上传DataFrame到MinIO（作为CSV文件）"""
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        data = io.BytesIO(csv_bytes)
        data.seek(0)  # 确保指针回到起点

        try:
            self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_name,
                data=data,
                length=len(csv_bytes),
                content_type='text/csv'
            )
            self.logger.info(f"Uploaded DataFrame to {self.bucket}/{object_name}")
        except S3Error as e:
            self.logger.error(f"Upload DataFrame failed: {str(e)}")
            raise
