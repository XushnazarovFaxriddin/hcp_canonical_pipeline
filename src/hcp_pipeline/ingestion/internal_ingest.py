from pyspark.sql import SparkSession
from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger
from pathlib import Path

logger = get_logger("internal_ingest")

def main():
    settings = load_project_settings()
    spark = SparkSession.builder.appName("internal_ingest").config("spark.sql.warehouse.dir", "./_work/spark-warehouse").config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=./_work/hadoop").getOrCreate()

    input_path = settings["paths"]["input"]["internal_jsonl"]
    bronze_path = settings["paths"]["working"]["bronze_internal"]

    logger.info(f"Reading Internal JSONL: {input_path}")
    df = spark.read.json(input_path)

    df = df.withColumn("source_system", df.source_system)  # passthrough
    logger.info(f"Writing bronze Internal to: {bronze_path}")
    df.write.mode("overwrite").parquet(bronze_path)

    spark.stop()

if __name__ == "__main__":
    main()
