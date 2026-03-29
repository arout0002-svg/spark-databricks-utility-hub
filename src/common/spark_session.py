"""Spark session bootstrap helper with Delta support."""

from typing import Any, Dict

from pyspark.sql import SparkSession


def get_spark(app_name: str, extra_conf: Dict[str, Any] | None = None) -> SparkSession:
    """Create or return an existing SparkSession configured for Delta Lake."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(str(key), str(value))

    return builder.getOrCreate()
