"""Crypto market analysis batch job."""
# mypy: disable-error-code=no-redef

import argparse
import logging
from datetime import datetime, timezone
from typing import Any, cast

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from src.common.config_loader import get_required, load_yaml_config
    from src.common.delta_utils import add_ingest_columns, write_delta
    from src.common.logger import get_logger
    from src.common.spark_session import get_spark
    from src.common.validation import check_required_columns
except ModuleNotFoundError:
    try:
        from common.config_loader import get_required, load_yaml_config
        from common.delta_utils import add_ingest_columns, write_delta
        from common.logger import get_logger
        from common.spark_session import get_spark
        from common.validation import check_required_columns
    except ModuleNotFoundError:
        # Final fallback for serverless script execution contexts where package
        # imports are unavailable; keep the job runnable with local helpers.
        def load_yaml_config(config_path: str) -> dict[str, Any]:
            with open(config_path, "r", encoding="utf-8") as file:
                data = yaml.safe_load(file) or {}
            if not isinstance(data, dict):
                raise ValueError(f"Invalid YAML config format: {config_path}")
            return data

        def get_required(config: dict[str, Any], key: str) -> Any:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")
            return config[key]

        def check_required_columns(df: DataFrame, required_columns: list[str]) -> None:  # type: ignore[misc]
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                raise ValueError(f"Missing required columns: {missing}")

        def add_ingest_columns(df: DataFrame) -> DataFrame:
            return (
                df.withColumn("ingest_ts", F.current_timestamp())
                .withColumn("ingest_date", F.to_date(F.current_timestamp()))
            )

        def write_delta(df: DataFrame, path: str, mode: str = "append") -> None:  # type: ignore[misc]
            df.write.format("delta").mode(mode).save(path)

        def get_spark(app_name: str) -> SparkSession:  # type: ignore[misc]
            return SparkSession.builder.appName(app_name).getOrCreate()

        def get_logger(name: str, level: str = "INFO") -> logging.Logger:  # type: ignore[misc]
            logger = logging.getLogger(name)
            if logger.handlers:
                return logger
            logger.setLevel(level.upper())
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
            logger.addHandler(handler)
            logger.propagate = False
            return logger


def compute_indicators(df: DataFrame) -> DataFrame:
    """Compute moving average, RSI, and MACD-style metrics."""
    w14 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-13, 0)
    w12 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-11, 0)
    w26 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-25, 0)
    w9 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-8, 0)

    price_diff = F.col("close") - F.lag("close", 1).over(Window.partitionBy("symbol").orderBy("event_time"))

    df = df.withColumn("moving_avg_14", F.avg("close").over(w14))
    df = df.withColumn("gain", F.when(price_diff > 0, price_diff).otherwise(F.lit(0.0)))
    df = df.withColumn("loss", F.when(price_diff < 0, -price_diff).otherwise(F.lit(0.0)))
    df = df.withColumn("avg_gain_14", F.avg("gain").over(w14))
    df = df.withColumn("avg_loss_14", F.avg("loss").over(w14))
    df = df.withColumn(
        "rsi_14",
        F.when(F.col("avg_loss_14") == 0, F.lit(100.0)).otherwise(
            100 - (100 / (1 + (F.col("avg_gain_14") / F.col("avg_loss_14"))))
        ),
    )
    df = df.withColumn("ema_fast_12", F.avg("close").over(w12))
    df = df.withColumn("ema_slow_26", F.avg("close").over(w26))
    df = df.withColumn("macd", F.col("ema_fast_12") - F.col("ema_slow_26"))
    df = df.withColumn("macd_signal_9", F.avg("macd").over(w9))
    return df.drop("gain", "loss", "avg_gain_14", "avg_loss_14")


def main(config_path: str) -> None:
    """Entrypoint for Databricks crypto market analysis job."""
    logger = get_logger("crypto_market_analysis")
    spark = get_spark("crypto_market_analysis")
    logger.info("Starting crypto market analysis job")

    try:
        config = load_yaml_config(config_path)
        input_path = get_required(config, "crypto_input_path")
        output_path = get_required(config, "crypto_output_path")
        input_format = config.get("crypto_input_format", "csv")

        # Best-effort JAR utility usage. In serverless-only environments, direct JVM JAR
        # attachment may be unavailable; continue processing with Python logic.
        try:
            utc_now = datetime.now(timezone.utc).isoformat()
            jvm = cast(Any, spark._jvm)
            epoch_ms = jvm.com.company.utils.DateUtils.toEpochMillis(utc_now)
            logger.info("JAR utility check completed", extra={"current_epoch_ms": epoch_ms})
        except Exception:
            logger.warning("JAR utility not available; continuing without JVM helper")

        reader = spark.read
        if input_format == "csv":
            df = reader.option("header", "true").option("inferSchema", "true").csv(input_path)
        elif input_format == "json":
            df = reader.option("inferSchema", "true").json(input_path)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")

        required_cols = ["symbol", "event_time", "close"]
        check_required_columns(df, required_cols)

        df = df.withColumn("event_time", F.to_timestamp("event_time")).withColumn("close", F.col("close").cast("double"))
        scored = compute_indicators(df)
        enriched = add_ingest_columns(scored)
        write_delta(enriched, output_path, mode="overwrite")

        logger.info("Crypto market analysis completed", extra={"output_path": output_path})
    except Exception:
        logger.exception("Crypto market analysis job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="src/configs/dev.yaml", help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)
