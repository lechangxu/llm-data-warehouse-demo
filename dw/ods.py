"""ODS layer: raw ingestion, schema-on-write, append-only, zero transformation.

Mirrors production practice: land source data verbatim with ingestion metadata,
so downstream cleaning (DWD) is always replayable from an immutable raw copy.
"""
from pyspark.sql import SparkSession, functions as F

from .paths import DATA_SAMPLE, ODS_NEWS


def build(spark: SparkSession) -> int:
    raw = (
        spark.read.option("header", True)
        .option("multiLine", False)
        .csv(str(DATA_SAMPLE))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    raw.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(ODS_NEWS))
    return spark.read.format("delta").load(str(ODS_NEWS)).count()


if __name__ == "__main__":
    from .spark import get_spark

    s = get_spark("ods")
    print(f"ods_news rows: {build(s)}")
    s.stop()
