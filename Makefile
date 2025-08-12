PY=python

.PHONY: demo test

demo:
	$(PY) -m hcp_pipeline.ingestion.npi_ingest
	$(PY) -m hcp_pipeline.ingestion.internal_ingest
	$(PY) -m hcp_pipeline.transform.normalize
	$(PY) -m hcp_pipeline.model.canonical

test:
	pytest -q
