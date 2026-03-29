"""Structured streaming demo job using Spark rate source."""

import argparse

from pyspark.sql import functions as F

from common.config_loader import get_required, load_yaml_config
from common.logger import get_logger
from common.spark_session import get_spark


def main(config_path: str) -> None:
    """Entrypoint for Databricks streaming job."""
    logger = get_logger("streaming_job")
    spark = get_spark("streaming_job")
    logger.info("Starting streaming job")

    try:
        config = load_yaml_config(config_path)
        output_path = get_required(config, "stream_output_path")
        checkpoint_path = get_required(config, "stream_checkpoint_path")
        rows_per_second = int(config.get("stream_rows_per_second", 5))
        run_seconds = int(config.get("stream_run_seconds", 60))

        source_df = spark.readStream.format("rate").option("rowsPerSecond", rows_per_second).load()
        transformed_df = (
            source_df.withColumn("symbol", F.when((F.col("value") % 2) == 0, F.lit("BTC")).otherwise(F.lit("ETH")))
            .withColumn("close", (F.col("value") % 200) + F.lit(10000.0))
            .withColumnRenamed("timestamp", "event_time")
            .select("event_time", "symbol", "close", "value")
        )

        query = (
            transformed_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="10 seconds")
            .start(output_path)
        )

        logger.info(
            "Streaming query started",
            extra={"output_path": output_path, "checkpoint_path": checkpoint_path},
        )
        query.awaitTermination(timeout=run_seconds)
        query.stop()
        logger.info("Streaming job completed")
    except Exception:
        logger.exception("Streaming job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="src/configs/dev.yaml", help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)
