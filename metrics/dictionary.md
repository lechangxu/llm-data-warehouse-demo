# 指标口径字典（metrics/dictionary.md）

ADS 层每个指标的唯一权威口径。改口径必须改本文件 + 对应测试断言，否则质量门会红。

## ads_daily_metrics（一行 = 一个交易日）

| 指标 | 口径 | 上游 | 断言位置 |
|---|---|---|---|
| `event_date` | 新闻事件日期（`event_time` 截断到日，UTC 朴素时间戳） | dwd_news | tests/test_pipeline.py |
| `total_articles` | 当日清洗后新闻条数（`dwd_news` 去重+去空标题后） | dws_ticker_daily.article_count 求和 | tests/test_pipeline.py |
| `active_tickers` | 当日有 ≥1 条新闻的不同 ticker 数 | dws_ticker_daily.ticker 去重计数 | tests/test_pipeline.py |
| `sentiment_net_all` | 当日全部 ticker 的 `sentiment_net` 之和（词典法，见下） | dws_ticker_daily.sentiment_net 求和 | tests/test_pipeline.py |
| `max_articles_single_ticker` | 当日单一 ticker 最大新闻条数 | dws_ticker_daily.article_count 取 max | tests/test_pipeline.py |
| `concentration_ratio` | `max_articles_single_ticker / total_articles`，保留 4 位小数 | 本层派生 | tests/test_pipeline.py |

## sentiment_net 口径（DWS 层，词典法）

- 标题切词（`\W+` 分割，小写）后与正/负词表求交集，`sentiment = |正词| − |负词|`
- 词表见 `dw/dws.py` 的 `POSITIVE` / `NEGATIVE`，**是弱标注参考**，非 LLM 评分
- LLM 评分版是 SFT 管线的训练目标（`sft/build_sft.py` 的 `sentiment` 任务用同一词典法做弱标签）

## 分层职责（每层一句话）

| 层 | 表 | 职责 | 清洗动作 |
|---|---|---|---|
| ODS | ods_news | 原始落地，零转换，可重放 | 仅加 `ingestion_ts` |
| DWD | dwd_news | 清洗、去重、规范化 | 去空标题 / id 去重留最早 / ticker 大写去空格 / 时间戳类型化 |
| DWS | dws_ticker_daily | 日 × ticker 粒度聚合 | article_count / sentiment_net / source_count / avg_body_len |
| ADS | ads_daily_metrics | 应用层日级宽表 | 见上表口径 |

## 对账口径（质量门 `dwd_reconciliation`）

种子数据含且仅含 4 条脏数据（2 条重复 id + 2 条空标题），`ODS − DWD = 4` 恒成立。
清洗规则变动导致多删/少删时门会红 —— 对账断言的是**差值**，不是行数猜测。
