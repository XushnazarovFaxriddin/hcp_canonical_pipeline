"""Build canonical output tables using pandas."""

from pathlib import Path

import pandas as pd

from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger


logger = get_logger("canonical")


def main() -> None:
    settings = load_project_settings()

    prov = pd.read_parquet(settings["paths"]["working"]["silver_providers"])
    site = pd.read_parquet(settings["paths"]["working"]["silver_sites"])
    xref = pd.read_parquet(settings["paths"]["working"]["silver_xref"])

    out_prov = settings["paths"]["output"]["gold_providers"]
    out_site = settings["paths"]["output"]["gold_sites"]
    out_xref = settings["paths"]["output"]["gold_xref"]
    out_view = settings["paths"]["output"]["gold_view"]

    logger.info(f"Writing gold providers: {out_prov}")
    prov_out = prov[
        ["provider_npi", "canonical_provider_name", "primary_specialty", "last_seen_source"]
    ].rename(columns={"last_seen_source": "source_system"})
    Path(out_prov).parent.mkdir(parents=True, exist_ok=True)
    prov_out.to_parquet(out_prov, index=False)

    logger.info(f"Writing gold sites: {out_site}")
    site_out = site[
        [
            "practice_site_id",
            "canonical_practice_name",
            "practice_line1",
            "practice_line2",
            "practice_city",
            "practice_state",
            "practice_postal",
            "last_seen_source",
        ]
    ].rename(columns={"last_seen_source": "source_system"})
    Path(out_site).parent.mkdir(parents=True, exist_ok=True)
    site_out.to_parquet(out_site, index=False)

    logger.info(f"Writing gold xref: {out_xref}")
    xref_out = xref[
        ["provider_npi", "practice_site_id", "relationship_type", "last_seen_source"]
    ].rename(columns={"last_seen_source": "source_system"})
    Path(out_xref).parent.mkdir(parents=True, exist_ok=True)
    xref_out.to_parquet(out_xref, index=False)

    view = (
        xref.merge(prov, on="provider_npi", how="left")
        .merge(site, on="practice_site_id", how="left")
    )
    view = view[
        [
            "provider_npi",
            "canonical_provider_name",
            "primary_specialty",
            "practice_site_id",
            "canonical_practice_name",
            "practice_line1",
            "practice_line2",
            "practice_city",
            "practice_state",
            "practice_postal",
            "last_seen_source",
        ]
    ].rename(columns={"last_seen_source": "source_system"})
    logger.info(f"Writing gold view: {out_view}")
    Path(out_view).parent.mkdir(parents=True, exist_ok=True)
    view.to_parquet(out_view, index=False)


if __name__ == "__main__":
    main()

