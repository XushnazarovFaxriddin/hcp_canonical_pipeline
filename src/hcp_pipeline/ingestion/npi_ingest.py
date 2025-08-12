from pyspark.sql import SparkSession
from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger
from pathlib import Path

logger = get_logger("npi_ingest")

def main():
    settings = load_project_settings()
    import os
    os.environ['HADOOP_HOME'] = os.path.abspath('./_work')
    spark = SparkSession.builder.appName("npi_ingest").config("spark.sql.warehouse.dir", "./_work/spark-warehouse").config("spark.hadoop.fs.permissions.enabled", "false").config("spark.sql.adaptive.enabled", "false").getOrCreate()

    input_path = settings["paths"]["input"]["npi_jsonl"]
    bronze_path = settings["paths"]["working"]["bronze_npi"]

    logger.info(f"Reading NPI JSONL: {input_path}")
    df = spark.read.json(input_path)

    df = df.withColumn("source_system", df.source_system)  # passthrough
    logger.info(f"Writing bronze NPI to: {bronze_path}")
    df.write.mode("overwrite").parquet(bronze_path)

    spark.stop()

if __name__ == "__main__":
    main()
