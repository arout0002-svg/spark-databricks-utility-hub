"""Delta Lake helper methods for reusable ETL patterns."""

from typing import Iterable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_by_keys(df: DataFrame, keys: Iterable[str], order_by_col: str) -> DataFrame:
    """Keep latest record per key using descending order column."""
    key_list: List[str] = list(keys)
    if not key_list:
        raise ValueError("At least one key is required for deduplication")

    w = Window.partitionBy(*key_list).orderBy(F.col(order_by_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_delta(df: DataFrame, path: str, mode: str = "append", partition_by: str | None = None) -> None:
    """Write DataFrame to Delta format with optional partitioning."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)


def merge_upsert_sql(
    target_table: str, source_view: str, join_condition: str, set_clause: str, insert_clause: str
) -> str:
    """Build Delta merge SQL statement for reusable merge/upsert workflows."""
    return f"""
MERGE INTO {target_table} AS target
USING {source_view} AS source
ON {join_condition}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT {insert_clause}
"""


def run_merge(spark: SparkSession, merge_sql: str) -> None:
    """Execute a prepared Delta merge SQL statement."""
    spark.sql(merge_sql)


def add_ingest_columns(df: DataFrame) -> DataFrame:
    """Attach standard ingestion metadata columns."""
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )
