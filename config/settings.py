from pathlib import Path


class Config:
    # 本地数据目录
    DATA_DIR = Path("D:/liner/dashuju/data")

    # MinIO (模拟OSS) 配置
    MINIO_ENDPOINT = "localhost:9003"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    MINIO_BUCKET = "ecommerce-data"
    MINIO_SECURE = False

    # ClickHouse 配置
    CH_HOST = "localhost"
    CH_PORT = 9004
    CH_USER = "root"
    CH_PASSWORD = "ea907"
    CH_CONNECT_ARGS = {'compression': False, 'settings': {'use_numpy': False}}


settings = Config()