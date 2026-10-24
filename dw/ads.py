"""ADS layer: application-facing daily market metrics.

Wide, flat, query-ready — one row per day. Every metric's 口径 (definition) is
documented in metrics/dictionary.md and asserted in tests/test_pipeline.py.
"""
from pyspark.sql import SparkSession, functions as F

from .paths import ADS_DAILY_METRICS, DWS_TICKER_DAILY


def build(spark: SparkSession) -> int:
    dws = spark.read.format("delta").load(str(DWS_TICKER_DAILY))

    top_source_share = (
        spark.read.format("delta").load(str(DWS_TICKER_DAILY))
        .groupBy("event_date")
        .agg(F.sum("article_count").alias("_day_total"))
    )

    daily = dws.groupBy("event_date").agg(
        F.sum("article_count").alias("total_articles"),
        F.countDistinct("ticker").alias("active_tickers"),
        F.sum("sentiment_net").alias("sentiment_net_all"),
        F.max("article_count").alias("max_articles_single_ticker"),
    )

    ads = (
        daily.join(top_source_share, "event_date")
        .withColumn("concentration_ratio", F.round(F.col("max_articles_single_ticker") / F.col("_day_total"), 4))
        .drop("_day_total")
        .orderBy("event_date")
    )

    ads.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(ADS_DAILY_METRICS))
    return spark.read.format("delta").load(str(ADS_DAILY_METRICS)).count()


if __name__ == "__main__":
    from .spark import get_spark

    s = get_spark("ads")
    print(f"ads_daily_metrics rows: {build(s)}")
    s.stop()
