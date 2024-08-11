from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from viper.config import CACHE_DIR, PARQUET_FILE

@dataclass
class EnvironmentScanner:

    cache_dir: str = CACHE_DIR
    parquet_file: str = PARQUET_FILE

    def __init__(self, **kwargs):
        super().__init__(kwargs)
        # Initialize a Spark session
        self.spark = SparkSession.builder \
            .appName("ViperScan") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()

    def scan(self):
        raise NotImplementedError()
