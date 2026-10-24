"""End-to-end pipeline tests: layer row counts, metric 口径 assertions, quality gates, SFT schema.

One Spark session per module (session-scoped fixture) — pipeline runs once, all tests
assert against the resulting Delta tables.
"""
import json

import pytest

from dw import ads, dwd, dws, ods
from dw.paths import ADS_DAILY_METRICS, DWD_NEWS, DWS_TICKER_DAILY, ODS_NEWS, SFT_JSONL
from dw.spark import get_spark
from quality.gates import run_all
from sft.build_sft import build as build_sft

# Seed CSV ground truth: 42 raw rows; 2 duplicate ids + 2 null titles => 38 clean rows.
RAW_ROWS = 42
CLEAN_ROWS = 38
SEED_DATES = 5  # 2026-10-19 .. 2026-10-23


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-pipeline")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def built(spark):
    counts = {
        "ods": ods.build(spark),
        "dwd": dwd.build(spark),
        "dws": dws.build(spark),
        "ads": ads.build(spark),
    }
    return spark, counts


def test_layer_row_counts(built):
    spark, counts = built
    assert counts["ods"] == RAW_ROWS
    assert counts["dwd"] == CLEAN_ROWS
    assert counts["dws"] > 0
    assert counts["ads"] == SEED_DATES


def test_dwd_conformed(built):
    spark, _ = built
    df = spark.read.format("delta").load(str(DWD_NEWS))
    # ticker conformed: no lowercase, no surrounding whitespace
    assert df.filter("ticker != upper(trim(ticker))").count() == 0
    # event_date derived correctly
    assert df.filter("event_date != to_date(event_time)").count() == 0


def test_ads_metric_definitions(built):
    """concentration_ratio == max_articles_single_ticker / total_articles (4dp rounding)."""
    spark, _ = built
    df = spark.read.format("delta").load(str(ADS_DAILY_METRICS))
    bad = df.filter(
        "abs(concentration_ratio - round(max_articles_single_ticker / total_articles, 4)) > 1e-9"
    ).count()
    assert bad == 0
    # active_tickers never exceeds total_articles
    assert df.filter("active_tickers > total_articles").count() == 0


def test_dws_sentiment_range(built):
    """Lexicon sentiment is bounded: |sentiment_net| <= article_count * max lexicon hits per title."""
    spark, _ = built
    df = spark.read.format("delta").load(str(DWS_TICKER_DAILY))
    assert df.filter("abs(sentiment_net) > article_count * 4").count() == 0


def test_quality_gates_all_pass(built):
    spark, _ = built
    failures = run_all(spark)
    assert failures == [], f"quality gates failed: {failures}"


def test_sft_jsonl_schema_and_count(built):
    spark, _ = built
    n = build_sft(spark)
    assert n == CLEAN_ROWS * 3  # 3 task shapes per article
    required = {"instruction", "input", "output", "task", "source_id"}
    tasks = set()
    with open(SFT_JSONL, encoding="utf-8") as f:
        lines = f.read().splitlines()
    assert len(lines) == n
    for line in lines:
        ex = json.loads(line)
        assert required <= ex.keys()
        assert all(isinstance(ex[k], str) and ex[k] for k in ("instruction", "input", "output"))
        tasks.add(ex["task"])
    assert tasks == {"headline", "ticker_tag", "sentiment"}
