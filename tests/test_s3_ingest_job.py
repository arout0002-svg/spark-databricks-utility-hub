"""Unit tests for the S3 crypto ingest job."""

import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from jobs.s3_ingest_job import configure_s3a, generate_sample_data, write_to_s3


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


def test_configure_s3a_reads_from_databricks_secrets(spark: SparkSession) -> None:
    """configure_s3a should prefer Databricks Secret Scope over env vars."""
    logger = logging.getLogger("test")
    fake_conf = MagicMock()

    fake_secrets = MagicMock()
    fake_secrets.get.side_effect = lambda scope, key: (
        "AKIA_FROM_SCOPE" if key == "aws_access_key_id" else "SECRET_FROM_SCOPE"
    )
    fake_dbutils = MagicMock()
    fake_dbutils.secrets = fake_secrets

    with patch("jobs.s3_ingest_job._get_dbutils", return_value=fake_dbutils):
        with patch.object(spark.sparkContext._jsc, "hadoopConfiguration", return_value=fake_conf):
            configure_s3a(spark, logger)

    set_calls = {call.args[0]: call.args[1] for call in fake_conf.set.call_args_list}
    assert set_calls.get("fs.s3a.access.key") == "AKIA_FROM_SCOPE"
    assert set_calls.get("fs.s3a.secret.key") == "SECRET_FROM_SCOPE"
    assert set_calls.get("fs.s3a.impl") == "org.apache.hadoop.fs.s3a.S3AFileSystem"


def test_configure_s3a_falls_back_to_env_vars(spark: SparkSession) -> None:
    """configure_s3a should use env vars when no dbutils / secrets are available."""
    logger = logging.getLogger("test")
    fake_conf = MagicMock()
    env = {"AWS_ACCESS_KEY_ID": "AKIAIOSFTEST", "AWS_SECRET_ACCESS_KEY": "wJalrXTest"}

    with patch("jobs.s3_ingest_job._get_dbutils", return_value=None):
        with patch.dict(os.environ, env):
            with patch.object(spark.sparkContext._jsc, "hadoopConfiguration", return_value=fake_conf):
                configure_s3a(spark, logger)

    set_calls = {call.args[0]: call.args[1] for call in fake_conf.set.call_args_list}
    assert set_calls.get("fs.s3a.access.key") == "AKIAIOSFTEST"
    assert set_calls.get("fs.s3a.secret.key") == "wJalrXTest"


def test_configure_s3a_raises_without_credentials(spark: SparkSession) -> None:
    """configure_s3a should raise EnvironmentError when no creds are found anywhere."""
    logger = logging.getLogger("test")

    with patch("jobs.s3_ingest_job._get_dbutils", return_value=None):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            with pytest.raises(EnvironmentError, match="aws-creds"):
                configure_s3a(spark, logger)
