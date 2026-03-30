"""Crypto market analysis batch job."""

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.config_loader import get_required, load_yaml_config
from common.delta_utils import add_ingest_columns, write_delta
from common.logger import get_logger
from common.spark_session import get_spark
from common.validation import check_required_columns


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
            epoch_ms = spark._jvm.com.company.utils.DateUtils.toEpochMillis(utc_now)
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
