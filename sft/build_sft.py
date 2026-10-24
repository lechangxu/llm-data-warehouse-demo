"""Build SFT training examples from the conformed DWD layer.

Three deterministic task shapes per article (ground truth comes from data fields,
no LLM in the loop — eval configs under sft/ are for scoring a *model's* output
against these same references):

1. headline   : body -> title              (summarization)
2. ticker_tag : title+body -> ticker       (entity extraction)
3. sentiment  : title -> POS/NEG/NEUTRAL   (classification, lexicon label as weak reference)

Output: data/output/sft.jsonl, one JSON object per line with
{instruction, input, output, task, source_id}.
"""
import json
from typing import Iterator

from pyspark.sql import SparkSession

from dw.dws import NEGATIVE, POSITIVE
from dw.paths import DWD_NEWS, SFT_JSONL

TASKS = ("headline", "ticker_tag", "sentiment")


def _weak_sentiment(title: str) -> str:
    words = set(title.lower().split())
    pos = len(words & POSITIVE)
    neg = len(words & NEGATIVE)
    if pos > neg:
        return "POSITIVE"
    if neg > pos:
        return "NEGATIVE"
    return "NEUTRAL"


def _examples(row) -> Iterator[dict]:
    base = {"source_id": row.id}
    yield {
        **base,
        "task": "headline",
        "instruction": "Write a concise news headline for the following article body.",
        "input": row.body,
        "output": row.title,
    }
    yield {
        **base,
        "task": "ticker_tag",
        "instruction": "Identify the primary ticker symbol discussed in this news item. Answer with the symbol only.",
        "input": f"{row.title}\n{row.body}",
        "output": row.ticker,
    }
    yield {
        **base,
        "task": "sentiment",
        "instruction": "Classify the sentiment of this headline as POSITIVE, NEGATIVE, or NEUTRAL.",
        "input": row.title,
        "output": _weak_sentiment(row.title),
    }


def build(spark: SparkSession) -> int:
    rows = spark.read.format("delta").load(str(DWD_NEWS)).collect()
    SFT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(SFT_JSONL, "w", encoding="utf-8") as f:
        for row in rows:
            for ex in _examples(row):
                f.write(json.dumps(ex, ensure_ascii=False) + "\n")
                n += 1
    return n


if __name__ == "__main__":
    from dw.spark import get_spark

    s = get_spark("sft-build")
    print(f"sft.jsonl examples: {build(s)}")
    s.stop()
