import pytest
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from common.validation import check_required_columns, validate_column_types, validate_schema_exact


def test_check_required_columns_success(spark):
    df = spark.createDataFrame([Row(id="1", value=10.0)])
    check_required_columns(df, ["id", "value"])


def test_check_required_columns_failure(spark):
    df = spark.createDataFrame([Row(id="1")])
    with pytest.raises(ValueError):
        check_required_columns(df, ["id", "value"])


def test_validate_schema_exact_success(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame([Row(id="1", value=10.0)], schema=schema)
    validate_schema_exact(df, schema)


def test_validate_column_types(spark):
    df = spark.createDataFrame([Row(id="1", value=10.0)])
    validate_column_types(df, {"id": "string", "value": "double"})
