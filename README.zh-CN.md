# llm-data-warehouse-demo

[![tests](https://github.com/lechangxu/llm-data-warehouse-demo/actions/workflows/tests.yml/badge.svg)](https://github.com/lechangxu/llm-data-warehouse-demo/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[English README](README.md)

**一个端到端迷你现代数仓：Delta Lake 四层建模（ODS→DWD→DWS→ADS）+ 可执行数据质量门 + LLM SFT 训练数据集产出 + 有测试盯着的指标口径字典。**

为「大模型训练/评测数据管线」场景演示数仓建模方法论。刻意本地规模 —— 交付物是分层纪律、对账门与指标治理，不是行数。

## 快速开始

需要 Java 17+（macOS：`brew install openjdk@17`）与 Python 3.12。

```bash
uv venv --python 3.12 .venv          # 或 python3.12 -m venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
./scripts/run_pipeline.sh            # ODS→DWD→DWS→ADS + 质量门 + sft.jsonl
.venv/bin/python -m pytest tests/    # 分层行数、指标口径、质量门、SFT schema
```

示例输出：

```text
[layer] ods_news: 42 rows
[layer] dwd_news: 38 rows
[layer] dws_ticker_daily: 34 rows
[layer] ads_daily_metrics: 5 rows
[quality] all gates passed
[sft] sft.jsonl: 114 examples
```

## 架构

```text
data/sample/news_seed.csv          （合成财经新闻，42 行含 4 条已知脏数据）
        │
        ▼
  ┌──────────┐   原始落地，只加 ingestion_ts，零转换，可重放
  │ ods_news │   Delta Lake
  └──────────┘
        ▼
  ┌──────────┐   id 去重 · 去空标题 · ticker 规范化 · 时间戳类型化
  │ dwd_news │   对账门：ODS−DWD 必须恰好等于 4 条已知脏数据
  └──────────┘
        ▼
┌──────────────────┐  日 × ticker 聚合：article_count / sentiment_net / source_count
│ dws_ticker_daily │  Delta Lake
└──────────────────┘
        ▼
┌───────────────────┐  应用层日级宽表；每个指标口径写在 metrics/dictionary.md，
│ ads_daily_metrics │  并有测试断言盯防漂移
└───────────────────┘
        ▼
  sft/build_sft.py → data/output/sft.jsonl（headline / ticker_tag / sentiment × 38 篇 = 114 条）
```

## 亮点

- **带对账的四层建模** —— DWD 门断言原始层与清洗层的**差值**恰等于已知脏数据条数；清洗规则悄悄多删/少删会当场红，而不是绿灯放行。
- **可执行质量门**（`quality/gates.py`）：空值率、唯一性、规范化、对账、跨层日期覆盖。验不了的判据不算门；任一失败管线非零退出。
- **LLM 数据管线**：SFT 样本从清洗层生成、ground truth 全部取自数据字段（无 LLM 参与标注）；`sft/promptfooconfig.yaml` 用同一批参考评分模型输出（本地档，不进 CI）。
- **指标治理**：`metrics/dictionary.md` 是 ADS 口径唯一权威来源；代码与字典漂移测试即红（如 `concentration_ratio` 的 4 位舍入有断言）。

## 仓结构

| 路径 | 内容 |
|---|---|
| `dw/` | 各层构建器（`ods/dwd/dws/ads.py`）、Spark+Delta 会话工厂、编排器（`pipeline.py`） |
| `quality/gates.py` | 命名的、可独立运行的质量门 |
| `sft/` | SFT 数据集构建器 + promptfoo 评测配置（本地档） |
| `metrics/dictionary.md` | 指标口径字典，含上游血缘与测试位置 |
| `data/sample/` | 合成种子数据（无真实行情、无版权内容） |
| `tests/` | 分层行数、指标口径、质量门、SFT schema |

## 技术栈

| 领域 | 选型 | 备注 |
|---|---|---|
| 计算 | PySpark 4.0（local） | 生产等价物：EMR / Databricks / Flink CDC 接入 |
| 表格式 | Delta Lake 4.0 | 每层 ACID + schema 强制 + time travel |
| 数据质量 | 自写门（pytest 断言盯防） | 与 Great Expectations 同哲学，演示零依赖 |
| LLM 评测 | promptfoo（本地档） | CI 只跑 schema/形状断言 —— CI 不放 LLM key |
| 语言 | Python 3.12 | Spark 运行需 Java 17 |

## 诚实边界

本仓是**本地规模的方法论演示**：42 行种子、单机。它演示分层纪律、对账门与指标治理 —— 不演示 TB 级运维。生产级主张在我的工作履历里：3 年 BI/数仓，含一个部级共享数据库数仓项目 —— 我负责其指标体系（4 主题域 / 70 分析维度 / 368 张治理报表），`metrics/dictionary.md` 编码的「一指标一口径」纪律即源自该项目实践。

## License

MIT —— 见 [LICENSE](LICENSE)。全部数据为合成。
