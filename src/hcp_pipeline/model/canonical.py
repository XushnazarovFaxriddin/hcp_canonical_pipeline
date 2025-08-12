from pyspark.sql import SparkSession, functions as F
from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger

logger = get_logger("canonical")

def main():
    settings = load_project_settings()
    spark = SparkSession.builder.appName("canonical").getOrCreate()

    prov = spark.read.parquet(settings["paths"]["working"]["silver_providers"])
    site = spark.read.parquet(settings["paths"]["working"]["silver_sites"])
    xref = spark.read.parquet(settings["paths"]["working"]["silver_xref"])

    out_prov = settings["paths"]["output"]["gold_providers"]
    out_site = settings["paths"]["output"]["gold_sites"]
    out_xref = settings["paths"]["output"]["gold_xref"]
    out_view = settings["paths"]["output"]["gold_view"]

    # Write canonical tables
    logger.info(f"Writing gold providers: {out_prov}")
    prov.select(
        "provider_npi","canonical_provider_name","primary_specialty","last_seen_source"
    ).withColumnRenamed("last_seen_source","source_system")     .write.mode("overwrite").parquet(out_prov)

    logger.info(f"Writing gold sites: {out_site}")
    site.select(
        "practice_site_id","canonical_practice_name",
        "practice_line1","practice_line2","practice_city","practice_state","practice_postal",
        "last_seen_source"
    ).withColumnRenamed("last_seen_source","source_system")     .write.mode("overwrite").parquet(out_site)

    logger.info(f"Writing gold xref: {out_xref}")
    xref.select("provider_npi","practice_site_id","relationship_type","last_seen_source")        .withColumnRenamed("last_seen_source","source_system")        .write.mode("overwrite").parquet(out_xref)

    # Build simple downstream view
    view = (xref.alias("x")
            .join(prov.alias("p"), "provider_npi", "left")
            .join(site.alias("s"), "practice_site_id", "left")
            .select(
                F.col("x.provider_npi"),
                F.col("p.canonical_provider_name"),
                F.col("p.primary_specialty"),
                F.col("s.practice_site_id"),
                F.col("s.canonical_practice_name"),
                F.col("s.practice_line1"),
                F.col("s.practice_line2"),
                F.col("s.practice_city"),
                F.col("s.practice_state"),
                F.col("s.practice_postal"),
                F.col("x.last_seen_source").alias("source_system"),
            ))

    logger.info(f"Writing gold view: {out_view}")
    view.write.mode("overwrite").parquet(out_view)

    spark.stop()

if __name__ == "__main__":
    main()
