"""Aggregate user role counts from S3 Parquet source data and write results to S3."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = "s3://s3-source-dt/users_parquet/"
OUTPUT_PATH = "s3://destination-dt/dest/s3output"


def main() -> None:
    """Read source user Parquet files, count roles per user, and save output."""
    spark = (
        SparkSession.builder.appName("UsersCountryCountS3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    try:
        result_df = (
            spark.read.parquet(INPUT_PATH)
            .select("name", "role")
            .groupBy("name")
            .agg(F.count("role").alias("cnt"))
        )

        result_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
