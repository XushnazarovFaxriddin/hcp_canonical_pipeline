"""Build canonical output tables using pandas."""

from pathlib import Path

import pandas as pd

from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger


logger = get_logger("canonical")


def main() -> None:
    # Get project root directory
    project_root = Path(__file__).resolve().parents[3]
    settings = load_project_settings()

    # Convert paths to absolute
    prov = pd.read_parquet(project_root / settings["paths"]["working"]["silver_providers"])
    site = pd.read_parquet(project_root / settings["paths"]["working"]["silver_sites"])
    xref = pd.read_parquet(project_root / settings["paths"]["working"]["silver_xref"])

    out_prov = project_root / settings["paths"]["output"]["gold_providers"]
    out_site = project_root / settings["paths"]["output"]["gold_sites"]
    out_xref = project_root / settings["paths"]["output"]["gold_xref"]
    out_view = project_root / settings["paths"]["output"]["gold_view"]

    logger.info(f"Writing gold providers: {out_prov}")
    prov_out = prov[
        ["provider_npi", "canonical_provider_name", "primary_specialty", "src"]
    ].rename(columns={"src": "source_system"})
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
            "src",  # Using src instead of last_seen_source
        ]
    ].rename(columns={"src": "source_system"})
    logger.info(f"Writing gold view: {out_view}")
    Path(out_view).parent.mkdir(parents=True, exist_ok=True)
    view.to_parquet(out_view, index=False)


if __name__ == "__main__":
    main()

