"""DWD layer: cleaned, deduplicated, conformed detail data.

Cleaning rules (each is asserted by quality/gates.py):
- drop rows with null/empty title (unverifiable wire items)
- dedupe on id, keeping the earliest event_time
- normalize ticker: trim + uppercase
- cast event_time to timestamp, derive event_date
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from .paths import DWD_NEWS, ODS_NEWS


def build(spark: SparkSession) -> int:
    ods = spark.read.format("delta").load(str(ODS_NEWS))

    cleaned = (
        ods.filter(F.length(F.trim(F.coalesce(F.col("title"), F.lit("")))) > 0)
        .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("body", F.trim(F.col("body")))
    )

    w = Window.partitionBy("id").orderBy(F.col("event_time").asc())
    deduped = (
        cleaned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("event_date", F.to_date("event_time"))
    )

    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(DWD_NEWS))
    return spark.read.format("delta").load(str(DWD_NEWS)).count()


if __name__ == "__main__":
    from .spark import get_spark

    s = get_spark("dwd")
    print(f"dwd_news rows: {build(s)}")
    s.stop()
