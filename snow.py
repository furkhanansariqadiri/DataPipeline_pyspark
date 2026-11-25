"""Read Snowflake USERS table using AWS Secrets Manager credentials and aggregate counts to S3."""

import json
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SECRET_ID = "snowpass"
OUTPUT_PATH = "s3://destination-dt/dest/snowOutput"


def get_snowflake_password(secret_id: str) -> str:
    """Fetch the Snowflake password securely from AWS Secrets Manager."""
    result = subprocess.run(
        [
            "aws",
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            secret_id,
            "--output",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    secret_response = json.loads(result.stdout)
    return json.loads(secret_response["SecretString"])["snowpass"]


def main() -> None:
    """Load Snowflake USERS table, count COUNTRY entries per USERNAME, and write Parquet."""
    snowpwd = get_snowflake_password(SECRET_ID)

    sf_options = {
        "sfURL": "https://coumuui-wu38497.snowflakecomputing.com",
        "sfAccount": "COUMUUI-WU38497",
        "sfUSER": "ANSARIFURKHAN",
        "sfPassword": snowpwd,
        "sfdatabase": "ANSARIDB",
        "sfSchema": "ANSARISCHEMA",
        "sfrole": "ACCOUNTADMIN",
        "sfWarehouse": "COMPUTE_WH",
        "dbtable": "USERS",
    }

    spark = (
        SparkSession.builder.appName("Snowflake Job")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    try:
        # Project only needed columns before aggregation to enable Snowflake column pruning.
        result_df = (
            spark.read.format("snowflake")
            .options(**sf_options)
            .load()
            .select("USERNAME", "COUNTRY")
            .groupBy("USERNAME")
            .agg(F.count("COUNTRY").alias("cnt"))
        )

        result_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
