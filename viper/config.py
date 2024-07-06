import os

OSV_DATASET_URL = os.getenv(
    "VIPER_OSV_DATASET_URL",
    "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip"
)

CACHE_DIR = os.getenv(
    "VIPER_CACHE_DIR",
    ".viper_cache"
)

PARQUET_FILE = os.getenv(
    "VIPER_PARQUET_FILE",
    "vulnerabilities.parquet"
)