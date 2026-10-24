"""Shared paths for the warehouse layout. All Delta tables live under warehouse/."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_SAMPLE = ROOT / "data" / "sample" / "news_seed.csv"
WAREHOUSE = ROOT / "warehouse"
OUTPUT = ROOT / "data" / "output"

ODS_NEWS = WAREHOUSE / "ods_news"
DWD_NEWS = WAREHOUSE / "dwd_news"
DWS_TICKER_DAILY = WAREHOUSE / "dws_ticker_daily"
ADS_DAILY_METRICS = WAREHOUSE / "ads_daily_metrics"
SFT_JSONL = OUTPUT / "sft.jsonl"
