"""Pipeline orchestrator: ODS -> DWD -> DWS -> ADS, then quality gates + SFT export.

Usage: python -m dw.pipeline
Exit code is non-zero if any quality gate fails — this is what CI runs.
"""
from quality.gates import run_all
from sft.build_sft import build as build_sft

from . import ads, dwd, dws, ods
from .spark import get_spark


def main() -> None:
    spark = get_spark("llm-dw-pipeline")
    try:
        counts = {
            "ods_news": ods.build(spark),
            "dwd_news": dwd.build(spark),
            "dws_ticker_daily": dws.build(spark),
            "ads_daily_metrics": ads.build(spark),
        }
        for table, n in counts.items():
            print(f"[layer] {table}: {n} rows")

        failures = run_all(spark)
        if failures:
            for f in failures:
                print(f"[quality] FAIL {f}")
            raise SystemExit(1)
        print("[quality] all gates passed")

        n = build_sft(spark)
        print(f"[sft] sft.jsonl: {n} examples")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
