from pyspark.sql import SparkSession, functions as F
from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger
from src.hcp_pipeline.utils.hashing import stable_site_id
from pathlib import Path

logger = get_logger("normalize")


def normalize_address(df, prefix, line1, line2, city, state, postal):
    # very light normalization
    return df.withColumn(prefix + "_line1_norm", F.upper(F.trim(F.col(line1))))\
            .withColumn(prefix + "_line2_norm", F.upper(F.trim(F.col(line2))))\
            .withColumn(prefix + "_city_norm",  F.upper(F.trim(F.col(city))))\
            .withColumn(prefix + "_state_norm", F.upper(F.trim(F.col(state))))\
            .withColumn(prefix + "_postal_norm", F.upper(F.trim(F.col(postal))))
            


def main():
    settings = load_project_settings()
    spark = SparkSession.builder.appName("normalize").config("spark.sql.warehouse.dir", "./_work/spark-warehouse").config("spark.hadoop.fs.permissions.enabled", "false").getOrCreate()

    bronze_npi = settings["paths"]["working"]["bronze_npi"]
    bronze_internal = settings["paths"]["working"]["bronze_internal"]
    out_prov = settings["paths"]["working"]["silver_providers"]
    out_site = settings["paths"]["working"]["silver_sites"]
    out_xref = settings["paths"]["working"]["silver_xref"]

    npi = spark.read.parquet(bronze_npi)
    internal = spark.read.parquet(bronze_internal)

    # ---- Providers (union NPI + Internal) ----
    npi_p = (npi
             .select(
                 F.col("npi").alias("provider_npi"),
                 F.col("provider_name").alias("provider_name_npi"),
                 F.col("primary_specialty").alias("specialty_npi"),
                 F.lit("NPI Registry").alias("src")
             ))

    internal_p = (internal
                  .select(
                      F.col("treating_provider_npi").alias("provider_npi"),
                      F.col("treating_provider_name").alias(
                          "provider_name_internal"),
                      F.lit(None).cast("string").alias("specialty_internal"),
                      F.lit("InternalPatientSystem").alias("src")
                  ))

    providers = (npi_p.unionByName(internal_p, allowMissingColumns=True)
                 .groupBy("provider_npi")
                 .agg(
                     F.first("provider_name_npi", ignorenulls=True).alias("provider_name_npi"),
                     F.first("provider_name_internal", ignorenulls=True).alias("provider_name_internal"),
                     F.first("specialty_npi", ignorenulls=True).alias("specialty_npi"),
                     F.max("src").alias("last_seen_source")
                 )
                 .withColumn("canonical_provider_name",
                           F.coalesce(F.col("provider_name_npi"), F.col("provider_name_internal")))
                 .withColumn("primary_specialty", F.col("specialty_npi")))

    logger.info(f"Writing silver providers: {out_prov}")
    providers.write.mode("overwrite").parquet(out_prov)

    # ---- Sites (derive from both) ----
    # NPI sites
    npi_s = normalize_address(
        npi,
        prefix="practice",
        line1="practice_address.line1",
        line2="practice_address.line2",
        city="practice_address.city",
        state="practice_address.state",
        postal="practice_address.postal_code",
    ).select(
        F.col("practice_line1_norm").alias("line1"),
        F.col("practice_line2_norm").alias("line2"),
        F.col("practice_city_norm").alias("city"),
        F.col("practice_state_norm").alias("state"),
        F.col("practice_postal_norm").alias("postal"),
        F.col("practice_name").alias("practice_name"),
        F.lit("NPI Registry").alias("src")
    )

    # Internal sites
    internal_s = normalize_address(
        internal,
        prefix="enc",
        line1="encounter_location_address.line1",
        line2="encounter_location_address.line2",
        city="encounter_location_address.city",
        state="encounter_location_address.state",
        postal="encounter_location_address.postal_code",
    ).select(
        F.col("enc_line1_norm").alias("line1"),
        F.col("enc_line2_norm").alias("line2"),
        F.col("enc_city_norm").alias("city"),
        F.col("enc_state_norm").alias("state"),
        F.col("enc_postal_norm").alias("postal"),
        F.col("encounter_location_name").alias("practice_name"),
        F.lit("InternalPatientSystem").alias("src")
    )

    sites = npi_s.unionByName(internal_s, allowMissingColumns=True)

    # derive site id via UDF
    spark.udf.register("stable_site_id", stable_site_id)
    sites = sites.withColumn(
        "practice_site_id",
        F.expr("stable_site_id(line1, city, state, postal, line2)")
    )

    sites = (sites
             .groupBy("practice_site_id")
             .agg(
                 F.first("practice_name", ignorenulls=True).alias(
                     "canonical_practice_name"),
                 F.first("line1", ignorenulls=True).alias("practice_line1"),
                 F.first("line2", ignorenulls=True).alias("practice_line2"),
                 F.first("city", ignorenulls=True).alias("practice_city"),
                 F.first("state", ignorenulls=True).alias("practice_state"),
                 F.first("postal", ignorenulls=True).alias("practice_postal"),
                 F.max("src").alias("last_seen_source"),
             ))

    logger.info(f"Writing silver sites: {out_site}")
    sites.write.mode("overwrite").parquet(out_site)

    # ---- Xref (providers↔sites) ----
    # join internal encounters to provider_npi and site id
    internal_norm = normalize_address(
        internal,
        prefix="enc",
        line1="encounter_location_address.line1",
        line2="encounter_location_address.line2",
        city="encounter_location_address.city",
        state="encounter_location_address.state",
        postal="encounter_location_address.postal_code",
    ).withColumn(
        "practice_site_id",
        F.expr("stable_site_id(upper(trim(encounter_location_address.line1)), "
               "upper(trim(encounter_location_address.city)), "
               "upper(trim(encounter_location_address.state)), "
               "upper(trim(encounter_location_address.postal_code)), "
               "upper(trim(encounter_location_address.line2)))")
    )

    xref = (internal_norm
            .select(
                F.col("treating_provider_npi").alias("provider_npi"),
                F.col("practice_site_id"),
                F.lit("provider_at").alias("relationship_type"),
                F.lit("InternalPatientSystem").alias("last_seen_source")
            ).dropna(subset=["provider_npi", "practice_site_id"])
            .dropDuplicates(["provider_npi", "practice_site_id"])
            )

    logger.info(f"Writing silver provider-site link: {out_xref}")
    xref.write.mode("overwrite").parquet(out_xref)

    spark.stop()


if __name__ == "__main__":
    main()
