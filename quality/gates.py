"""Data quality gates — every gate is a named, independently-runnable assertion.

Design rule: a gate that cannot actually be executed is not a gate. Each check here
returns None on pass or a human-readable failure string; run_all aggregates and the
pipeline exits non-zero on any failure (same philosophy as the 13-point backtest
hygiene checklist this demo cites in the README).
"""
from dataclasses import dataclass
from typing import Callable, Optional

from pyspark.sql import SparkSession, functions as F

from dw.paths import ADS_DAILY_METRICS, DWD_NEWS, DWS_TICKER_DAILY, ODS_NEWS


@dataclass
class Gate:
    name: str
    run: Callable[[SparkSession], Optional[str]]  # None = pass


def _no_nulls(table, column):
    def check(spark: SparkSession) -> Optional[str]:
        n = spark.read.format("delta").load(str(table)).filter(F.col(column).isNull()).count()
        return f"{table.name}.{column} has {n} nulls" if n else None

    return check


def _unique(table, column):
    def check(spark: SparkSession) -> Optional[str]:
        df = spark.read.format("delta").load(str(table))
        dup = df.count() - df.select(column).distinct().count()
        return f"{table.name}.{column} has {dup} duplicates" if dup else None

    return check


def _min_rows(table, minimum: int):
    def check(spark: SparkSession) -> Optional[str]:
        n = spark.read.format("delta").load(str(table)).count()
        return f"{table.name} has {n} rows (< {minimum})" if n < minimum else None

    return check


def _dwd_dropped_dirty_rows(spark: SparkSession) -> Optional[str]:
    """Reconciliation gate: ODS - DWD row delta must equal known dirty-row count.

    The seed CSV contains exactly 4 dirty rows (2 duplicate ids, 2 null titles).
    If cleaning silently drops more (or fewer), something changed upstream — fail loud.
    """
    ods_n = spark.read.format("delta").load(str(ODS_NEWS)).count()
    dwd_n = spark.read.format("delta").load(str(DWD_NEWS)).count()
    dropped = ods_n - dwd_n
    if dropped != 4:
        return f"ODS->DWD dropped {dropped} rows, expected exactly 4 (2 dup ids + 2 null titles)"
    return None


def _ads_covers_all_dates(spark: SparkSession) -> Optional[str]:
    dws_dates = {r.event_date for r in spark.read.format("delta").load(str(DWS_TICKER_DAILY)).select("event_date").distinct().collect()}
    ads_dates = {r.event_date for r in spark.read.format("delta").load(str(ADS_DAILY_METRICS)).select("event_date").distinct().collect()}
    missing = dws_dates - ads_dates
    return f"ADS missing dates: {sorted(map(str, missing))}" if missing else None


GATES = [
    Gate("ods_not_empty", _min_rows(ODS_NEWS, 1)),
    Gate("dwd_no_null_title", _no_nulls(DWD_NEWS, "title")),
    Gate("dwd_id_unique", _unique(DWD_NEWS, "id")),
    Gate("dwd_ticker_conformed", lambda s: (
        "dwd_news.ticker not trimmed/upper" if s.read.format("delta").load(str(DWD_NEWS))
        .filter(F.col("ticker") != F.upper(F.trim(F.col("ticker")))).count() else None)),
    Gate("dwd_reconciliation", _dwd_dropped_dirty_rows),
    Gate("dws_not_empty", _min_rows(DWS_TICKER_DAILY, 1)),
    Gate("ads_covers_all_dates", _ads_covers_all_dates),
]


def run_all(spark: SparkSession) -> list[str]:
    """Returns list of failure strings; empty = all green."""
    failures = []
    for gate in GATES:
        result = gate.run(spark)
        if result:
            failures.append(f"{gate.name}: {result}")
    return failures
