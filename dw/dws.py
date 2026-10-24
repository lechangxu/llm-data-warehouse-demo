"""DWS layer: daily per-ticker aggregation (the reusable service layer).

Sentiment here is a transparent lexicon score (documented in metrics/dictionary.md),
deliberately simple so the demo stays deterministic — the LLM-scored variant is what
the SFT pipeline (sft/build_sft.py) produces training data for.
"""
from pyspark.sql import SparkSession, functions as F

from .paths import DWD_NEWS, DWS_TICKER_DAILY

POSITIVE = {"unveils", "beats", "record", "growth", "raise", "raises", "wins", "inflows", "stabilize", "double", "doubled", "expand", "improves", "accelerates", "partnership", "opens", "milestone", "recovery", "outpaced", "high"}
NEGATIVE = {"recalls", "fault", "shortage", "probe", "drops", "compresses", "uncertain", "restrict", "outage", "low", "fell", "declined", "outflows", "lacking"}


def _lexicon_score(text_col):
    words = F.split(F.lower(text_col), r"\W+")
    pos = F.size(F.array_intersect(words, F.array([F.lit(w) for w in sorted(POSITIVE)])))
    neg = F.size(F.array_intersect(words, F.array([F.lit(w) for w in sorted(NEGATIVE)])))
    return (pos - neg).cast("int")


def build(spark: SparkSession) -> int:
    dwd = spark.read.format("delta").load(str(DWD_NEWS))
    scored = dwd.withColumn("sentiment", _lexicon_score(F.col("title")))

    agg = scored.groupBy("event_date", "ticker").agg(
        F.count("*").alias("article_count"),
        F.sum("sentiment").alias("sentiment_net"),
        F.countDistinct("source").alias("source_count"),
        F.avg(F.length("body")).cast("int").alias("avg_body_len"),
    )

    agg.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(DWS_TICKER_DAILY))
    return spark.read.format("delta").load(str(DWS_TICKER_DAILY)).count()


if __name__ == "__main__":
    from .spark import get_spark

    s = get_spark("dws")
    print(f"dws_ticker_daily rows: {build(s)}")
    s.stop()
