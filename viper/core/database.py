from dataclasses import dataclass, field
from zipfile import ZipFile
from io import BytesIO

import httpx
from viper.config import OSV_DATASET_URL

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType
)

from typing import List, LiteralString
import pathlib


@dataclass
class OSVDatabase:

    cache_dir: str = ".viper_cache"
    num_partitions: int = 10
    partition_columns: List[LiteralString] = field(default_factory=lambda: ["package_name", "versions"])

    def __init__(self, **kwargs):
        # Initialize a Spark session
        self.spark = SparkSession.builder \
            .appName("OSVDatabase") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()


    def download(self, cache_dir: str = ".viper_cache"):

        response = httpx.get(OSV_DATASET_URL)
        zip_file = ZipFile(BytesIO(response.content))
        zip_file.extractall(cache_dir)

    def preprocess(self, output_parquet_file: str = "vulnerabilities.parquet"):

        # Define the schema to avoid the overhead of schema inference
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("details", StringType(), True),
            StructField("aliases", ArrayType(StringType()), True),
            StructField("modified", TimestampType(), True),
            StructField("published", TimestampType(), True),
            StructField("database_specific", StructType([
                StructField("severity", StringType(), True)
            ]), True),
            StructField("affected", ArrayType(StructType([
                StructField("package", StructType([
                    StructField("name", StringType(), True)
                ]), True),
                StructField("versions", ArrayType(StringType()), True)
            ])), True),
            StructField("severity", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("score", StringType(), True)
            ])), True)
        ])

        df = self.spark.read.option("multiLine", "true").schema(schema).json(self.cache_dir)
        # Select relevant fields, handling nested structures appropriately
        selected_df = df.select(
            F.col("id"),
            F.col("summary"),
            F.col("details"),
            F.col("aliases"),
            F.col("modified"),
            F.col("published"),
            F.col("database_specific.severity").alias("severity"),
            F.col("affected.package.name").alias("package_name"),
            F.col("affected.versions").alias("versions"),
            F.col("severity.score").alias("severity_score")
        )

        #Explode array fields
        exploded_df = (
            selected_df
                .withColumn("package_name", F.explode(F.col("package_name")))
                .withColumn("versions", F.explode(F.col("versions")))
                .withColumn("severity_score", F.explode(F.col("severity_score")))
        )

        filtered_df = (
            exploded_df
                .filter(F.col("versions").isNull() is False)
                .withColumn(
                    "versions",
                    F.explode(F.col("versions"))
                )
        )

        filtered_df = filtered_df.repartition(
            self.num_partitions,
            self.partition_columns
        )

        path = pathlib.Path(self.cache_dir).joinpath(output_parquet_file)
        filtered_df.write.mode("overwrite").parquet(str(path))