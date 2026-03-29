"""DataFrame validation helpers for schema and data quality checks."""

from typing import Dict, Iterable

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def check_required_columns(df: DataFrame, required_columns: Iterable[str]) -> None:
    """Validate that required columns are present in DataFrame."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_schema_exact(df: DataFrame, expected_schema: StructType) -> None:
    """Validate DataFrame schema exactly matches expected schema."""
    if df.schema != expected_schema:
        raise ValueError(f"Schema mismatch. Expected: {expected_schema}, Found: {df.schema}")


def validate_column_types(df: DataFrame, expected_types: Dict[str, str]) -> None:
    """Validate selected DataFrame columns have expected data types."""
    actual = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    mismatches = {
        col: {"expected": exp_type, "actual": actual.get(col)}
        for col, exp_type in expected_types.items()
        if actual.get(col) != exp_type
    }
    if mismatches:
        raise ValueError(f"Column type mismatches detected: {mismatches}")
