"""Ingest NPI registry data using pandas."""

from pathlib import Path

import pandas as pd

from src.hcp_pipeline.utils.io import load_project_settings
from src.hcp_pipeline.utils.logging import get_logger


logger = get_logger("npi_ingest")


def _read_jsonl(path: str, chunksize: int | None = 100_000) -> pd.DataFrame:
    """Read a JSONL file into a DataFrame with optional chunking."""

    try:
        if chunksize:
            iterator = pd.read_json(path, lines=True, chunksize=chunksize)
            df = pd.concat(iterator, ignore_index=True)
        else:
            df = pd.read_json(path, lines=True)
    except ValueError:
        df = pd.read_json(path, lines=True)
    return df


def main() -> None:
    settings = load_project_settings()

    input_path = settings["paths"]["input"]["npi_jsonl"]
    bronze_path = settings["paths"]["working"]["bronze_npi"]

    logger.info(f"Reading NPI JSONL: {input_path}")
    df = _read_jsonl(input_path)

    # passthrough column for schema compatibility
    df["source_system"] = df["source_system"]

    logger.info(f"Writing bronze NPI to: {bronze_path}")
    Path(bronze_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_path, index=False)


if __name__ == "__main__":
    main()

