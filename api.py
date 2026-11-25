"""Fetch random users from the RandomUser API and write flattened Parquet output to S3."""

import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

API_URL = "https://randomuser.me/api?results=5"
OUTPUT_PATH = "s3://destination-dt/dest/apiOutput"


def main() -> None:
    """Fetch API JSON, flatten user fields, and persist as Parquet."""
    spark = (
        SparkSession.builder.appName("RandomUserAPI")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    try:
        with urllib.request.urlopen(API_URL, timeout=30) as response:
            raw_data = response.read().decode("utf-8")

        # Single-partition RDD avoids creating empty partitions for a single JSON payload.
        rdd = spark.sparkContext.parallelize([raw_data], numSlices=1)
        df = spark.read.json(rdd)

        flat_df = (
            df.select(
                F.col("info.version").alias("version"),
                F.explode(F.col("results")).alias("user"),
            )
            .select(
                F.concat_ws(
                    " ",
                    F.col("user.name.title"),
                    F.col("user.name.first"),
                    F.col("user.name.last"),
                ).alias("name"),
                F.col("version"),
                F.col("user.location.country").alias("country"),
            )
        )

        flat_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
