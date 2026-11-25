"""Join API, S3, and Snowflake Parquet outputs into the final master dataset on S3."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

API_INPUT_PATH = "s3://destination-dt/dest/apiOutput/"
S3_INPUT_PATH = "s3://destination-dt/dest/s3output/"
SNOW_INPUT_PATH = "s3://destination-dt/dest/snowOutput/"
OUTPUT_PATH = "s3://destination-dt/dest/masterOutput"


def main() -> None:
    """Read upstream Parquet datasets, inner-join on name, and write master output."""
    spark = (
        SparkSession.builder.appName("MasterJoinJob")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    try:
        api_df = spark.read.parquet(API_INPUT_PATH).select("name", "version", "country")
        s3_df = spark.read.parquet(S3_INPUT_PATH).select(
            F.col("name"),
            F.col("cnt").alias("s3_cnt"),
        )
        snow_df = spark.read.parquet(SNOW_INPUT_PATH).select(
            F.col("USERNAME").alias("name"),
            F.col("cnt").alias("snow_cnt"),
        )

        # Broadcast the tiny API dataset to avoid shuffling the larger S3 dataset.
        master_df = (
            F.broadcast(api_df)
            .join(s3_df, on="name", how="inner")
            .join(snow_df, on="name", how="inner")
        )

        master_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
