"""Generic ETL batch job with quality checks."""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common.config_loader import get_required, load_yaml_config
from common.delta_utils import add_ingest_columns, deduplicate_by_keys, write_delta
from common.logger import get_logger
from common.spark_session import get_spark
from common.validation import check_required_columns, validate_column_types


def clean_data(df: DataFrame) -> DataFrame:
    """Trim all string columns and drop full-row duplicates."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df.dropDuplicates()


def main(config_path: str) -> None:
    """Entrypoint for Databricks ETL job."""
    logger = get_logger("etl_job")
    spark = get_spark("etl_job")
    logger.info("Starting ETL job")

    try:
        config = load_yaml_config(config_path)
        input_path = get_required(config, "etl_input_path")
        output_path = get_required(config, "etl_output_path")
        input_format = config.get("etl_input_format", "json")
        dedupe_keys = config.get("etl_dedupe_keys", ["id"])
        order_col = config.get("etl_dedupe_order_col", "updated_at")
        required_columns = config.get("etl_required_columns", ["id", "updated_at"])
        expected_types = config.get("etl_expected_types", {"id": "string"})

        if input_format == "json":
            raw_df = spark.read.option("multiLine", "false").json(input_path)
        elif input_format == "csv":
            raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        else:
            raise ValueError(f"Unsupported ETL input format: {input_format}")

        check_required_columns(raw_df, required_columns)
        cleaned_df = clean_data(raw_df)
        transformed_df = cleaned_df.withColumn("updated_at", F.to_timestamp("updated_at"))
        transformed_df = deduplicate_by_keys(transformed_df, dedupe_keys, order_col)
        validate_column_types(transformed_df, expected_types)

        final_df = add_ingest_columns(transformed_df)
        write_delta(final_df, output_path, mode="overwrite")

        logger.info("ETL job completed", extra={"output_path": output_path})
    except Exception:
        logger.exception("ETL job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="src/configs/dev.yaml", help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)
