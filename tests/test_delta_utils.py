from pyspark.sql import Row

from common.delta_utils import add_ingest_columns, deduplicate_by_keys, merge_upsert_sql


def test_deduplicate_by_keys_keeps_latest(spark):
    rows = [
        Row(id="1", updated_at="2026-01-01 00:00:00", value=10),
        Row(id="1", updated_at="2026-01-02 00:00:00", value=20),
        Row(id="2", updated_at="2026-01-01 00:00:00", value=30),
    ]
    df = spark.createDataFrame(rows)
    result = deduplicate_by_keys(df, ["id"], "updated_at")
    assert result.count() == 2
    assert result.filter("id = '1'").collect()[0]["value"] == 20


def test_add_ingest_columns(spark):
    df = spark.createDataFrame([Row(id="1")])
    result = add_ingest_columns(df)
    assert "ingest_ts" in result.columns
    assert "ingest_date" in result.columns


def test_merge_upsert_sql_contains_merge_keywords():
    sql = merge_upsert_sql(
        target_table="main.default.target",
        source_view="source_view",
        join_condition="target.id = source.id",
        set_clause="target.value = source.value",
        insert_clause="(id, value) VALUES (source.id, source.value)",
    )
    assert "MERGE INTO" in sql
    assert "WHEN MATCHED" in sql
    assert "WHEN NOT MATCHED" in sql
