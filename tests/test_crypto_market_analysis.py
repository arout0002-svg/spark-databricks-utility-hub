from pyspark.sql import Row
from pyspark.sql import functions as F

from jobs.crypto_market_analysis import compute_indicators


def test_compute_indicators_adds_expected_columns(spark):
    rows = [
        Row(symbol="BTC", event_time="2026-01-01 00:00:00", close=100.0),
        Row(symbol="BTC", event_time="2026-01-01 00:01:00", close=102.0),
        Row(symbol="BTC", event_time="2026-01-01 00:02:00", close=101.0),
        Row(symbol="BTC", event_time="2026-01-01 00:03:00", close=105.0),
    ]
    df = spark.createDataFrame(rows).withColumn("event_time", F.to_timestamp("event_time"))
    result = compute_indicators(df)

    expected_cols = {"moving_avg_14", "rsi_14", "macd", "macd_signal_9"}
    assert expected_cols.issubset(set(result.columns))
