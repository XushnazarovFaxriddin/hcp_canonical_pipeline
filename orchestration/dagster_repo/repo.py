from dagster import Definitions, job, op
import subprocess, sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]

@op
def ingest_sources():
    subprocess.check_call([sys.executable, "-m", "src.hcp_pipeline.ingestion.npi_ingest"], cwd=ROOT)
    subprocess.check_call([sys.executable, "-m", "src.hcp_pipeline.ingestion.internal_ingest"], cwd=ROOT)

@op
def build_canonical():
    subprocess.check_call([sys.executable, "-m", "src.hcp_pipeline.transform.normalize"], cwd=ROOT)
    subprocess.check_call([sys.executable, "-m", "src.hcp_pipeline.model.canonical"], cwd=ROOT)

@job
def hcp_pipeline_job():
    # Define dependency: build_canonical should run after ingest_sources
    # but we don't pass the output as input since these ops don't return data
    ingest_sources()
    build_canonical()

defs = Definitions(jobs=[hcp_pipeline_job])
