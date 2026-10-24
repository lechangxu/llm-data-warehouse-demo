"""Spark session factory with Delta Lake wired in.

Local-only configuration: single driver, 2g memory, warehouse dir inside the repo.
Production equivalents (EMR/Databricks/Flink CDC ingestion) are discussed in the README.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .paths import WAREHOUSE


def get_spark(app_name: str = "llm-dw-demo") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        # macOS hosts whose Bonjour hostname doesn't resolve break the driver bind;
        # pinning loopback is harmless on CI too.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
