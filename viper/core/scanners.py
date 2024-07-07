from dataclasses import dataclass
from pyspark.sql import SparkSession

from viper.config import CACHE_DIR, PARQUET_FILE

@dataclass
class EnvironmentScanner:

    cache_dir: str = CACHE_DIR
    parquet_file: str = PARQUET_FILE

    def __init__(self, **kwargs):

        # Initialize a Spark session - TODO: centralize in a Singleton class
        self.spark = SparkSession.builder \
            .appName("Read JSON Files") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()

    def scan(self):
        raise NotImplementedError()
    
@dataclass
class MlFlowScanner:

    def __init__(self, **kwargs):
        raise NotImplementedError()
    

@dataclass
class HuggingFaceScanner:

    def __init__(self, **kwargs):
        raise NotImplementedError()
