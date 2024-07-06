from dataclasses import dataclass
from zipfile import ZipFile
from io import BytesIO

import httpx
from viper.config import OSV_DATASET_URL


@dataclass
class OSVDatabase:


    def download(self, cache_dir: str = ".viper_cache"):

        response = httpx.get(OSV_DATASET_URL)
        zip_file = ZipFile(BytesIO(response.content))
        zip_file.extractall(cache_dir)