# llm-data-warehouse-demo

[![tests](https://github.com/lechangxu/llm-data-warehouse-demo/actions/workflows/tests.yml/badge.svg)](https://github.com/lechangxu/llm-data-warehouse-demo/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[中文 README](README.zh-CN.md)

**An end-to-end miniature modern data warehouse: Delta Lake four-layer modeling (ODS→DWD→DWS→ADS), executable data-quality gates, LLM SFT dataset production, and a metric dictionary with testable definitions.**

Built to demonstrate data-warehouse modeling methodology for LLM training/eval data pipelines. Local-scale by design — the layering, reconciliation gates, and metric governance are the deliverable, not the row count.

## Quick Start

Requires Java 17+ (`brew install openjdk@17` on macOS) and Python 3.12.

```bash
uv venv --python 3.12 .venv          # or: python3.12 -m venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
./scripts/run_pipeline.sh            # ODS→DWD→DWS→ADS + quality gates + sft.jsonl
.venv/bin/python -m pytest tests/    # layer counts, metric definitions, quality gates, SFT schema
```

Example output:

```text
[layer] ods_news: 42 rows
[layer] dwd_news: 38 rows
[layer] dws_ticker_daily: 40 rows
[layer] ads_daily_metrics: 5 rows
[quality] all gates passed
[sft] sft.jsonl: 114 examples
```

## Architecture

```text
data/sample/news_seed.csv          (synthetic financial news, 42 rows incl. 4 known-dirty)
        │
        ▼
  ┌──────────┐   append-only raw landing, + ingestion_ts, zero transformation
  │ ods_news │   Delta Lake
  └──────────┘
        ▼
  ┌──────────┐   dedupe by id · drop null titles · ticker conformed · typed timestamps
  │ dwd_news │   reconciliation gate: ODS−DWD must equal exactly the 4 known-dirty rows
  └──────────┘
        ▼
┌──────────────────┐  day × ticker aggregation: article_count / sentiment_net / source_count
│ dws_ticker_daily │  Delta Lake
└──────────────────┘
        ▼
┌───────────────────┐  application-wide daily metrics; every definition lives in
│ ads_daily_metrics │  metrics/dictionary.md and is asserted in tests
└───────────────────┘
        ▼
  sft/build_sft.py → data/output/sft.jsonl  (headline / ticker_tag / sentiment × 38 articles = 114)
```

## Highlights

- **Four-layer modeling with reconciliation** — the DWD gate asserts the *delta* between raw and clean equals the known dirty-row count, so silent over/under-cleaning fails loud instead of passing green.
- **Executable quality gates** (`quality/gates.py`): null-rate, uniqueness, conformance, reconciliation, cross-layer date coverage. A gate that can't be executed isn't a gate; any failure exits the pipeline non-zero.
- **LLM data pipeline**: SFT examples are generated from the conformed layer with deterministic ground truth; `sft/promptfooconfig.yaml` scores a model against the same references (local档, not in CI).
- **Metric governance**: `metrics/dictionary.md` is the single source of truth for ADS definitions; tests fail if code and dictionary drift apart (e.g. `concentration_ratio` rounding is asserted).

## Repository Layout

| Path | What |
|---|---|
| `dw/` | Layer builders (`ods/dwd/dws/ads.py`), Spark+Delta session factory, orchestrator (`pipeline.py`) |
| `quality/gates.py` | Named, independently runnable quality gates |
| `sft/` | SFT dataset builder + promptfoo eval config (local) |
| `metrics/dictionary.md` | Metric definitions (口径) with upstream lineage and test pointers |
| `data/sample/` | Synthetic seed data (no real market data, no licensed content) |
| `tests/` | Layer counts, metric definitions, quality gates, SFT schema |

## Tech Stack

| Area | Choice | Note |
|---|---|---|
| Compute | PySpark 4.0 (local) | Production equivalents: EMR / Databricks / Flink CDC ingestion |
| Table format | Delta Lake 4.0 | ACID + schema enforcement + time travel at every layer |
| Quality | Custom gates (pytest-asserted) | Same philosophy as Great Expectations, zero-dependency for demo |
| LLM eval | promptfoo (local档) | CI runs schema/shape checks only — no LLM key in CI |
| Language | Python 3.12 | Java 17 runtime for Spark |

## Honest Scope

This is a **local-scale methodology demo**: 42 seed rows, single machine. It demonstrates layering discipline, reconciliation gating, and metric governance — not TB-scale operations. Production claims live in my work history: 3 years BI/data warehousing, including a national-scale shared-database DW program where I owned the metric system across 4 subject domains, 70 analysis dimensions and 368 governed reports — the same 口径-per-metric discipline that `metrics/dictionary.md` encodes here.

## License

MIT — see [LICENSE](LICENSE). All data is synthetic.
