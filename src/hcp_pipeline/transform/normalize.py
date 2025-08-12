"""Normalize and consolidate provider and site data using pandas."""

from pathlib import Path

import pandas as pd

from src.hcp_pipeline.utils.hashing import stable_site_id
from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger


logger = get_logger("normalize")


def normalize_address(df: pd.DataFrame, prefix: str, line1: str, line2: str, city: str, state: str, postal: str) -> pd.DataFrame:
    """Normalize basic address fields using pandas string operations."""

    df = df.copy()
    df[f"{prefix}_line1_norm"] = df[line1].fillna("").astype(str).str.upper().str.strip()
    df[f"{prefix}_line2_norm"] = df[line2].fillna("").astype(str).str.upper().str.strip()
    df[f"{prefix}_city_norm"] = df[city].fillna("").astype(str).str.upper().str.strip()
    df[f"{prefix}_state_norm"] = df[state].fillna("").astype(str).str.upper().str.strip()
    df[f"{prefix}_postal_norm"] = df[postal].fillna("").astype(str).str.upper().str.strip()
    return df


def _flatten_address(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Expand a nested address dictionary column into dotted columns."""

    if column not in df.columns:
        return df
    addr = pd.json_normalize(df[column]).add_prefix(f"{column}.")
    return df.drop(columns=[column]).join(addr)


def main() -> None:
    # Get project root directory
    project_root = Path(__file__).resolve().parents[3]
    settings = load_project_settings()

    # Convert relative paths to absolute
    bronze_npi = project_root / settings["paths"]["working"]["bronze_npi"]
    bronze_internal = project_root / settings["paths"]["working"]["bronze_internal"]
    out_prov = project_root / settings["paths"]["working"]["silver_providers"]
    out_site = project_root / settings["paths"]["working"]["silver_sites"]
    out_xref = project_root / settings["paths"]["working"]["silver_xref"]

    logger.info(f"Reading NPI data from: {bronze_npi}")
    try:
        npi = pd.read_parquet(bronze_npi)
        logger.info(f"NPI data shape: {npi.shape}")
    except Exception as e:
        logger.error(f"Failed to read NPI data: {e}")
        raise

    logger.info(f"Reading internal data from: {bronze_internal}")
    try:
        internal = pd.read_parquet(bronze_internal)
        logger.info(f"Internal data shape: {internal.shape}")
    except Exception as e:
        logger.error(f"Failed to read internal data: {e}")
        raise

    # Flatten nested address structures for ease of use
    npi = _flatten_address(npi, "practice_address")
    internal = _flatten_address(internal, "encounter_location_address")

    # ---- Providers ----
    # Get all columns to debug
    logger.info(f"Available columns in NPI data: {npi.columns.tolist()}")
    
    # Try to read with more flexible column selection
    npi_cols = ["npi", "provider_name", "primary_specialty"]
    available_cols = [col for col in npi_cols if col in npi.columns]
    
    logger.info(f"Looking for columns: {npi_cols}")
    logger.info(f"Found columns: {available_cols}")
    
    if not available_cols:
        logger.error("No expected columns found in NPI data")
        logger.error(f"Available columns: {npi.columns.tolist()}")
        raise ValueError(f"None of the expected columns {npi_cols} found in NPI data")
    
    npi_p = npi[available_cols].rename(columns={
        "npi": "provider_npi",
        "provider_name": "provider_name_npi",
        "primary_specialty": "specialty_npi",
    })
    npi_p["src"] = "NPI Registry"

    internal_p = internal[["treating_provider_npi", "treating_provider_name"]].rename(columns={
        "treating_provider_npi": "provider_npi",
        "treating_provider_name": "provider_name_internal",
    })
    internal_p["specialty_npi"] = None
    internal_p["src"] = "InternalPatientSystem"

    providers = pd.concat([npi_p, internal_p], ignore_index=True)
    providers = (
        providers.groupby("provider_npi", dropna=False)
        .agg(
            {
                "provider_name_npi": "first",
                "provider_name_internal": "first",
                "specialty_npi": "first",
                "src": "max",
            }
        )
        .reset_index()
    )
    providers["canonical_provider_name"] = providers["provider_name_npi"].combine_first(
        providers["provider_name_internal"]
    )
    providers["primary_specialty"] = providers["specialty_npi"]

    try:
        output_dir = Path(out_prov).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        # Set Windows-compatible permissions
        import stat
        output_dir.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        logger.info(f"Writing silver providers: {out_prov}")
        providers.to_parquet(out_prov, engine='pyarrow', compression=None, index=False)
    except PermissionError as e:
        logger.error(f"Permission denied when writing to {out_prov}")
        logger.error("Try running the script with administrator privileges")
        raise
    except Exception as e:
        logger.error(f"Failed to write providers file: {e}")
        raise

    # ---- Sites ----
    npi_s = normalize_address(
        npi,
        prefix="practice",
        line1="practice_address.line1",
        line2="practice_address.line2",
        city="practice_address.city",
        state="practice_address.state",
        postal="practice_address.postal_code",
    )
    npi_s = npi_s[
        [
            "practice_line1_norm",
            "practice_line2_norm",
            "practice_city_norm",
            "practice_state_norm",
            "practice_postal_norm",
            "practice_name",
        ]
    ].rename(
        columns={
            "practice_line1_norm": "line1",
            "practice_line2_norm": "line2",
            "practice_city_norm": "city",
            "practice_state_norm": "state",
            "practice_postal_norm": "postal",
        }
    )
    npi_s["src"] = "NPI Registry"

    internal_s = normalize_address(
        internal,
        prefix="enc",
        line1="encounter_location_address.line1",
        line2="encounter_location_address.line2",
        city="encounter_location_address.city",
        state="encounter_location_address.state",
        postal="encounter_location_address.postal_code",
    )
    internal_s = internal_s[
        [
            "enc_line1_norm",
            "enc_line2_norm",
            "enc_city_norm",
            "enc_state_norm",
            "enc_postal_norm",
            "encounter_location_name",
        ]
    ].rename(
        columns={
            "enc_line1_norm": "line1",
            "enc_line2_norm": "line2",
            "enc_city_norm": "city",
            "enc_state_norm": "state",
            "enc_postal_norm": "postal",
            "encounter_location_name": "practice_name",
        }
    )
    internal_s["src"] = "InternalPatientSystem"

    sites = pd.concat([npi_s, internal_s], ignore_index=True)
    sites["practice_site_id"] = sites.apply(
        lambda r: stable_site_id(r["line1"], r["city"], r["state"], r["postal"], r["line2"]),
        axis=1,
    )
    sites = (
        sites.groupby("practice_site_id", dropna=False)
        .agg(
            {
                "practice_name": "first",
                "line1": "first",
                "line2": "first",
                "city": "first",
                "state": "first",
                "postal": "first",
                "src": "max",
            }
        )
        .reset_index()
        .rename(
            columns={
                "practice_name": "canonical_practice_name",
                "line1": "practice_line1",
                "line2": "practice_line2",
                "city": "practice_city",
                "state": "practice_state",
                "postal": "practice_postal",
                "src": "last_seen_source",
            }
        )
    )

    try:
        output_dir = Path(out_site).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        # Set Windows-compatible permissions
        import stat
        output_dir.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        logger.info(f"Writing silver sites: {out_site}")
        sites.to_parquet(out_site, engine='pyarrow', compression=None, index=False)
    except PermissionError as e:
        logger.error(f"Permission denied when writing to {out_site}")
        logger.error("Try running the script with administrator privileges")
        raise
    except Exception as e:
        logger.error(f"Failed to write sites file: {e}")
        raise

    # ---- Xref ----
    internal_norm = normalize_address(
        internal,
        prefix="enc",
        line1="encounter_location_address.line1",
        line2="encounter_location_address.line2",
        city="encounter_location_address.city",
        state="encounter_location_address.state",
        postal="encounter_location_address.postal_code",
    )
    internal_norm["practice_site_id"] = internal_norm.apply(
        lambda r: stable_site_id(
            r["enc_line1_norm"],
            r["enc_city_norm"],
            r["enc_state_norm"],
            r["enc_postal_norm"],
            r["enc_line2_norm"],
        ),
        axis=1,
    )

    xref = internal_norm[["treating_provider_npi", "practice_site_id"]].dropna(
        subset=["treating_provider_npi", "practice_site_id"]
    )
    xref = xref.drop_duplicates()
    xref = xref.rename(columns={"treating_provider_npi": "provider_npi"})
    xref["relationship_type"] = "provider_at"
    xref["last_seen_source"] = "InternalPatientSystem"

    try:
        output_dir = Path(out_xref).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        # Set Windows-compatible permissions
        import stat
        output_dir.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        logger.info(f"Writing silver provider-site link: {out_xref}")
        xref.to_parquet(out_xref, engine='pyarrow', compression=None, index=False)
    except PermissionError as e:
        logger.error(f"Permission denied when writing to {out_xref}")
        logger.error("Try running the script with administrator privileges")
        raise
    except Exception as e:
        logger.error(f"Failed to write xref file: {e}")
        raise


if __name__ == "__main__":
    main()

