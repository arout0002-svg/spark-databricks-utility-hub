"""Unit tests for the S3 crypto ingest job."""

import pytest
from pyspark.sql import SparkSession

from jobs.s3_ingest_job import generate_sample_data, write_to_s3


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_s3_ingest_job")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def test_generate_sample_data_schema(spark: SparkSession) -> None:
    df = generate_sample_data(spark, num_hours=5)
    expected_cols = {"symbol", "event_time", "open", "high", "low", "close", "volume"}
    assert expected_cols.issubset(set(df.columns))


def test_generate_sample_data_row_count(spark: SparkSession) -> None:
    num_hours = 10
    tickers = 5
    df = generate_sample_data(spark, num_hours=num_hours)
    assert df.count() == num_hours * tickers


def test_generate_sample_data_price_positive(spark: SparkSession) -> None:
    df = generate_sample_data(spark, num_hours=3)
    min_close = df.agg({"close": "min"}).collect()[0][0]
    assert min_close > 0


def test_write_to_s3_local(spark: SparkSession, tmp_path: object) -> None:
    """Verify write_to_s3 creates parquet output on the local filesystem."""
    df = generate_sample_data(spark, num_hours=3)
    output_path = str(tmp_path) + "/bronze"  # type: ignore[operator]
    write_to_s3(df, output_path, partition_by="symbol")

    result = spark.read.parquet(output_path)
    assert result.count() == df.count()
    assert "symbol" in result.columns
