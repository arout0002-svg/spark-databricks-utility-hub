"""S3 crypto ingest job — generates sample market data and writes to S3 bronze layer."""
# mypy: disable-error-code=no-redef

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from src.common.config_loader import get_required, load_yaml_config
    from src.common.delta_utils import add_ingest_columns
    from src.common.logger import get_logger
    from src.common.spark_session import get_spark
except ModuleNotFoundError:
    try:
        from common.config_loader import get_required, load_yaml_config  # type: ignore[no-redef]
        from common.delta_utils import add_ingest_columns  # type: ignore[no-redef]
        from common.logger import get_logger  # type: ignore[no-redef]
        from common.spark_session import get_spark  # type: ignore[no-redef]
    except ModuleNotFoundError:
        # Serverless inline fallbacks — no package path available.
        def load_yaml_config(config_path: str) -> dict[str, Any]:  # type: ignore[misc]
            with open(config_path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            if not isinstance(data, dict):
                raise ValueError(f"Invalid YAML config: {config_path}")
            return data

        def get_required(config: dict[str, Any], key: str) -> Any:  # type: ignore[misc]
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")
            return config[key]

        def add_ingest_columns(df: DataFrame) -> DataFrame:  # type: ignore[misc]
            return (
                df.withColumn("ingest_ts", F.current_timestamp())
                .withColumn("ingest_date", F.to_date(F.current_timestamp()))
            )

        def get_spark(app_name: str) -> SparkSession:  # type: ignore[misc]
            return SparkSession.builder.appName(app_name).getOrCreate()

        def get_logger(name: str, level: str = "INFO") -> logging.Logger:  # type: ignore[misc]
            logger = logging.getLogger(name)
            if logger.handlers:
                return logger
            logger.setLevel(level.upper())
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
            logger.addHandler(handler)
            logger.propagate = False
            return logger


# Sample crypto tickers and their approximate base prices
_SAMPLE_TICKERS = [
    ("BTC", 62000.0),
    ("ETH", 3100.0),
    ("SOL", 140.0),
    ("BNB", 580.0),
    ("ADA", 0.45),
]


def generate_sample_data(spark: SparkSession, num_hours: int = 72) -> DataFrame:
    """Generate synthetic OHLCV crypto market data for the last N hours."""
    rows = []
    now = datetime.now(timezone.utc)
    for ticker, base_price in _SAMPLE_TICKERS:
        for h in range(num_hours):
            ts = (now - timedelta(hours=num_hours - h)).strftime("%Y-%m-%d %H:%M:%S")
            # Simple deterministic price walk
            offset = (h % 10 - 5) * base_price * 0.002
            open_price = round(base_price + offset, 8)
            high_price = round(open_price * 1.005, 8)
            low_price = round(open_price * 0.995, 8)
            close_price = round(open_price + (h % 3 - 1) * base_price * 0.001, 8)
            volume = round(1000.0 + (h * 7 % 500), 2)
            rows.append((ticker, ts, open_price, high_price, low_price, close_price, volume))

    return spark.createDataFrame(
        rows,
        ["symbol", "event_time", "open", "high", "low", "close", "volume"],
    )


def configure_s3a(spark: SparkSession, logger: logging.Logger) -> None:
    """Configure Spark's S3A filesystem with AWS credentials from env vars.

    Reads AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, which are injected by
    the Databricks job task environment_variables declared in jobs.yml.
    """
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()

    if not access_key or not secret_key:
        raise EnvironmentError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set. "
            "Configure them as GitHub secrets (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) "
            "and they will be forwarded to the Databricks job via bundle variables."
        )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    # Path-style access avoids DNS issues for buckets with dots in the name.
    hadoop_conf.set("fs.s3a.path.style.access", "false")
    # Enable AWS Signature Version 4 (required for many regions).
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    logger.info("S3A filesystem configured with explicit AWS credentials")


def write_to_s3(df: DataFrame, s3_path: str, partition_by: str = "symbol") -> None:
    """Write DataFrame to S3 in Parquet format, partitioned by symbol."""
    (
        df.write.format("parquet")
        .mode("overwrite")
        .partitionBy(partition_by)
        .save(s3_path)
    )


def main(config_path: str) -> None:
    """Entrypoint for the S3 crypto ingest job."""
    logger = get_logger("s3_ingest_job")
    spark = get_spark("s3_ingest_job")
    configure_s3a(spark, logger)
    logger.info("Starting S3 crypto ingest job")

    try:
        config = load_yaml_config(config_path)
        s3_output_path = get_required(config, "s3_bronze_output_path")
        num_hours = int(config.get("s3_ingest_num_hours", 72))
        input_path = config.get("crypto_input_path", "")
        input_format = config.get("crypto_input_format", "csv")

        # Try loading real input data from Volume; fall back to generated data.
        df: DataFrame
        try:
            if input_path:
                reader = spark.read
                if input_format == "csv":
                    df = reader.option("header", "true").option("inferSchema", "true").csv(input_path)
                else:
                    df = reader.option("inferSchema", "true").json(input_path)
                # Trigger an early check to detect missing path.
                df.limit(1).count()
                logger.info("Loaded real input data", extra={"input_path": input_path})
            else:
                raise FileNotFoundError("No input_path configured")
        except Exception:
            logger.warning("Input unavailable; using generated sample data")
            df = generate_sample_data(spark, num_hours=num_hours)

        enriched = add_ingest_columns(df)
        enriched = enriched.withColumn("event_time", F.to_timestamp("event_time"))

        logger.info("Writing data to S3 bronze layer", extra={"s3_output_path": s3_output_path})
        write_to_s3(enriched, s3_output_path)

        written_count = spark.read.parquet(s3_output_path).count()
        logger.info(
            "S3 ingest complete",
            extra={"output_path": s3_output_path, "rows_written": written_count},
        )
    except Exception:
        logger.exception("S3 ingest job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write crypto data to S3 bronze layer")
    parser.add_argument("--config", default="src/configs/dev.yaml", help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)
