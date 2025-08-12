[Pandas version of this repo](https://github.com/XushnazarovFaxriddin/hcp_canonical_pipeline/tree/pandas_version)

# Canonical HCP Data Pipeline (Prototype)

This repository contains a minimal-but-professional prototype for building a **canonical healthcare provider (HCP) model** from:
- **NPI Registry**-like JSON (simulated)
- **Internal Patient System**-like JSON (simulated)

It demonstrates ingestion, normalization/deduplication, entity linking, and a **gold-layer canonical model** suitable for downstream analytics, plus **basic lineage** and **source metadata**.

> Built with **PySpark**, structured as a modular Python package, with optional **Dagster** orchestration and **pytest** tests.

### Architecture diagram:
![diagram](/diagrams/architecture.png)

---

## 1) Problem Overview & Goals

- Ingest HCP data from two systems (NPI Registry & Internal Patient System)
- Normalize & link HCPs and practice sites using **NPI** where available and **address** matching as fallback
- Produce a **canonical model** for providers, sites/practices, and provider↔site relationships
- Output a simplified **gold view** for BI/app consumption, including preferred-source fields and lineage columns
- Add notes on **validation, trade-offs, and known gaps**

---

## 2) Data Schemas (Simulated)

### 2.1 NPI Registry (JSON line-per-record)
Fields (example subset):
- `npi` (string)
- `provider_name` (string) – full name or "LAST, FIRST"
- `primary_specialty` (string)
- `practice_name` (string, optional)
- `practice_address` (struct): `line1`, `line2`, `city`, `state`, `postal_code`
- `source_system` (string) – constant: `"NPI Registry"`

### 2.2 Internal Patient System (JSON line-per-record)
Fields (example subset):
- `internal_patient_id` (string)
- `treating_provider_npi` (string, nullable)
- `treating_provider_name` (string) – observed display name
- `encounter_location_name` (string)
- `encounter_location_address` (struct): `line1`, `line2`, `city`, `state`, `postal_code`
- `encounter_ts` (timestamp)
- `source_system` (string) – constant: `"InternalPatientSystem"`

> These schemas are intentionally simple but realistic. You can extend them if needed.

---

## 3) Canonical Model (Gold Layer)

### 3.1 Canonical Provider (`gold_canonical_providers`)
- `provider_npi` (string)
- `canonical_provider_name` (string) – name selection logic prefers NPI Registry
- `primary_specialty` (string) – prefer NPI Registry; fallback to internal if missing
- `first_seen_source` (string) – lineage
- `last_seen_source` (string) – lineage
- `record_start_ts` / `record_end_ts` (nullable) – SCD Type-2 friendly columns (future extension)

### 3.2 Canonical Practice Site (`gold_canonical_sites`)
- `practice_site_id` (string) – deterministic hash of standardized address (or surrogate UUID)
- `canonical_practice_name` (string, nullable)
- `practice_address` (flattened as normalized fields): `line1`, `line2`, `city`, `state`, `postal_code`
- `first_seen_source`, `last_seen_source`

### 3.3 Provider↔Site Relationship (`gold_provider_site_xref`)
- `provider_npi`
- `practice_site_id`
- `relationship_type` (e.g., "affiliated/provider_at")
- `first_seen_source`, `last_seen_source`

---

## 4) Normalization & Linking Rules (Summary)

1. **NPI-based linking**: If NPI is present and valid, we treat that as the provider identity key.
2. **Address normalization**: We trim, uppercase, collapse whitespace, standardize abbreviations (very light rules here), and form a deterministic **site_id** via hashing `line1|city|state|postal_code` (line2 optional).
3. **Name preference**: Prefer NPI Registry `provider_name` for canonical; otherwise fallback to internal `treating_provider_name` when no NPI data is available.
4. **Specialty preference**: Prefer NPI Registry `primary_specialty` when available; otherwise leave null or fallback to internal-derived value.
5. **Lineage**: Keep `(first_seen_source, last_seen_source)` at provider/site/relationship level.
6. **Deduplication**: Providers are deduped by NPI; sites are deduped by normalized address hash.

---

## 5) Output: Downstream Gold View

A combined **flattened view** (`gold_hcp_view`) with a single row per provider-site relationship:

- `provider_npi`
- `canonical_provider_name`
- `primary_specialty`
- `practice_site_id`
- `canonical_practice_name`
- `practice_line1`, `practice_line2`, `practice_city`, `practice_state`, `practice_postal_code`
- `source_system` (last seen for the row)  
- Plus optional audit fields: `ingest_date`, `build_id`

---

## 6) Orchestration (Dagster)

- A simple Dagster job with three ops: `ingest_sources` → `build_canonical` → `publish_gold`.
- Config-driven I/O paths in `src/hcp_pipeline/config/settings.yaml`.

Run locally:
```bash
pip install -r requirements.txt
# (Optional) set SPARK_HOME if needed for local PySpark
dagster dev -f orchestration/dagster_repo/repo.py
```

Or run modules directly:
```bash
python -m src.hcp_pipeline.ingestion.npi_ingest
python -m src.hcp_pipeline.ingestion.internal_ingest
python -m src.hcp_pipeline.transform.normalize
python -m src.hcp_pipeline.model.canonical
```

---

## 7) Tests

Run unit tests (small, illustrative):
```bash
pytest -q
```

---

## 8) Project Layout

```
hcp_canonical_pipeline/
├─ src/hcp_pipeline/
│  ├─ config/
│  │  └─ settings.yaml
│  ├─ ingestion/
│  │  ├─ npi_ingest.py
│  │  └─ internal_ingest.py
│  ├─ transform/
│  │  └─ normalize.py
│  ├─ model/
│  │  └─ canonical.py
│  └─ utils/
│     ├─ hashing.py
│     ├─ io.py
│     ├─ lineage.py
│     ├─ logging.py
│     └─ validation.py
├─ orchestration/dagster_repo/
│  └─ repo.py
├─ sample_data/
│  ├─ npi/
│  │  └─ npi_sample.jsonl
│  └─ internal/
│     └─ internal_sample.jsonl
├─ diagrams/
│  └─ architecture.mmd
├─ tests/
│  ├─ test_hashing.py
│  └─ test_normalize.py
├─ README.md
├─ requirements.txt
└─ Makefile
```

---

## 9) Known Gaps & Trade-offs
- Address standardization is a **light** implementation (no USPS/CASS-level logic).
- Relationship inference is simplistic (presence of an encounter implies affiliation).
- No SCD Type 2 yet; columns are present to extend later.
- Minimal Great Expectations-style validation (example hooks included).

---

## 10) How to Demo
1) Inspect `sample_data` JSONL files.
2) Run `make demo` to execute a local run that writes gold outputs to `./_output`.
3) Open `./_output/gold_hcp_view.parquet` in your BI tool or convert to CSV for quick peek.

---

## Additional Notes for Dagster

1. To persist information across sessions, set the `DAGSTER_HOME` environment variable to a directory to use for storage. For example:
   ```bash
   export DAGSTER_HOME=~/dagster_home
   ```
   On Windows, use:
   ```cmd
   set DAGSTER_HOME=C:\dagster_home
   ```

2. On Windows, to enable compute log capture, set the `PYTHONLEGACYWINDOWSSTDIO` environment variable:
   ```cmd
   set PYTHONLEGACYWINDOWSSTDIO=1
   ```
