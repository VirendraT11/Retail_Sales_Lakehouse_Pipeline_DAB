# Retail Sales Lakehouse Pipeline — Databricks Asset Bundles (DAB)

A production-style, end-to-end retail data pipeline built entirely on **Databricks Asset Bundles (DAB)**, implementing a full **Medallion architecture** (Bronze → Silver → Gold) on **Databricks Free Edition** with Unity Catalog, Lakeflow Declarative Pipelines (DLT), and serverless compute.

This project was built as a hands-on learning exercise to master DAB from first principles — covering bundle structure, variables/targets, jobs, DLT pipelines, cross-resource references, orchestration, and real production debugging scenarios.

---

## Architecture

```
                        ┌─────────────────────────────┐
                        │   ingestion_job (DAB job)    │
                        │  ┌────────────────────────┐  │
                        │  │ generate_dimensions.py │  │
                        │  └───────────┬────────────┘  │
                        │              ▼                │
                        │  ┌────────────────────────┐  │
                        │  │   generate_facts.py    │  │
                        │  └────────────────────────┘  │
                        └──────────────┬───────────────┘
                                       ▼
                    Bronze — retail_dab_dev.raw
        ┌───────────┬───────────┬───────────┬──────────────────┐
        │  stores    │ products  │ customers │ sales_transactions│
        │  (50)      │ (2,000)   │ (50,000)  │   (8,000,000)     │
        └───────────┴───────────┴───────────┴──────────────────┘
                                       │
                                       ▼  (DLT Pipeline)
                    Silver — retail_dab_dev.silver
                    ┌───────────────────────────────┐
                    │  sales_cleaned (materialized)  │
                    │  • dropDuplicates(transaction_id)
                    │  • @dlt.expect_or_drop x3       │
                    └───────────────┬───────────────┘
                                    ▼
                    Gold — retail_dab_dev.gold
        ┌────────────────────┬────────────────────────┬───────────────────────┐
        │ daily_sales_by_store│ daily_sales_by_category │ customer_lifetime_value│
        │     (45,000 rows)   │      (6,300 rows)        │      (50,000 rows)     │
        └────────────────────┴────────────────────────┴───────────────────────┘

        All orchestrated end-to-end by: retail_pipeline_orchestrator (DAB job)
```

---

## Tech Stack

| Component | Purpose |
|---|---|
| **Databricks Asset Bundles (DAB)** | Infrastructure-as-code for jobs, pipelines, schemas, permissions |
| **Databricks CLI v1.8.0** | Deploy, validate, and run bundles |
| **Lakeflow Declarative Pipelines (DLT)** | Silver/Gold transformations, data quality enforcement |
| **Unity Catalog** | Catalog/schema governance (`retail_dab_dev`) |
| **PySpark** | Distributed data generation and transformation logic |
| **Faker** | Realistic synthetic dimension data (stores, products, customers) |
| **Serverless Compute** | All jobs and pipelines run serverless (Free Edition compatible) |
| **uv** | Python wheel packaging for the bundle's `python_artifact` |

---

## Project Structure

```
retail_dab_project/
├── databricks.yml                     # Root bundle config: targets, variables
├── pyproject.toml                     # Python package definition (for wheel artifact)
├── resources/
│   ├── schemas.yml                    # UC schema resources (dev/prod + gold)
│   ├── ingestion_job.job.yml          # Bronze ingestion job (dimensions + facts)
│   ├── orchestrator_job.job.yml       # Top-level orchestrator (ingestion → DLT refresh)
│   ├── sample_job.job.yml             # Original scaffold job (Phase 1 learning artifact)
│   └── retail_dab_project_etl.pipeline.yml   # DLT pipeline config (Silver + Gold)
├── src/
│   ├── ingestion/
│   │   ├── generate_dimensions.py     # Notebook: stores, products, customers
│   │   └── generate_facts.py          # Notebook: 8M-row sales fact table (PySpark-native)
│   ├── retail_dab_project/            # Python wheel package (Phase 1 scaffold)
│   │   ├── main.py
│   │   └── taxis.py
│   └── retail_dab_project_etl/
│       └── transformations/
│           ├── silver_sales.py        # Silver: dedup + data quality (materialized view)
│           ├── gold_sales_by_store.py
│           ├── gold_sales_by_category.py
│           └── gold_customer_ltv.py
└── tests/                             # Unit tests (scaffold — not yet implemented)
```

---

## Data Model

**Bronze (`retail_dab_dev.raw`)** — synthetic data generated via Faker (dimensions) and pure PySpark distributed generation (`spark.range()`) for the 8M-row fact table, joined against real product pricing for realistic gross/discount/net amounts.

**Silver (`retail_dab_dev.silver`)** — `sales_cleaned`: deduplicated by `transaction_id`, enforced via three `@dlt.expect_or_drop` rules (valid quantity, valid net amount, valid discount percentage). Implemented as a **materialized view** (not a streaming table) since the upstream Bronze source is fully regenerated on each ingestion run rather than being append-only.

**Gold (`retail_dab_dev.gold`)** — three BI-ready, pre-joined, pre-aggregated tables:
- `daily_sales_by_store` — daily revenue/units/transactions per store, joined with store dimension attributes
- `daily_sales_by_category` — daily revenue/discount spend per product category
- `customer_lifetime_value` — all-time cumulative spend, order count, avg order value, return count, and purchase recency per customer

---

## Deployment

**Prerequisites:** Databricks CLI v1.8.0+, `uv`, a Databricks Free Edition workspace with Unity Catalog enabled, and a configured `~/.databrickscfg` profile.

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev (development mode: resource names prefixed, schedules paused)
databricks bundle deploy -t dev

# Run the entire pipeline end-to-end
databricks bundle run retail_pipeline_orchestrator -t dev
```

The orchestrator job runs `ingestion_job` (dimension + fact generation) followed by a `pipeline_task` refresh of the DLT pipeline (Silver + all Gold tables), fully sequenced via `depends_on`.

To run components independently for testing:

```bash
# Fast smoke test with a small transaction count
databricks bundle run ingestion_job -t dev --params num_transactions=10000

# Refresh only the DLT pipeline
databricks bundle run retail_dab_project_etl -t dev

# Full graph reset (required after structural changes, e.g. table type changes)
databricks bundle run retail_dab_project_etl -t dev --full-refresh-all
```

---

## Key DAB & DLT Concepts Demonstrated

- **Bundle anatomy**: `databricks.yml`, `resources/*.yml` auto-discovery, `artifacts` (wheel builds)
- **Variables & targets**: parameterizing catalog/schema per environment (`${var.X}`)
- **Cross-resource references**: `${resources.jobs.X.id}`, `${resources.pipelines.X.id}`, `${resources.schemas.X.name}` — resolving actual deployed identities rather than hardcoding
- **Two substitution systems**: `${...}` (deploy-time, DAB CLI) vs. `{{job.parameters.X}}` (run-time, Jobs engine)
- **Job orchestration**: `notebook_task`, `python_wheel_task`, `pipeline_task`, `run_job_task`, and `depends_on` DAGs
- **`mode: development`**: automatic resource name-prefixing and schedule-pausing — including its non-obvious effect on Unity Catalog schema names
- **DLT streaming tables vs. materialized views**: when each is appropriate, and why an append-only assumption breaks on a full-refresh source
- **Data quality enforcement**: `@dlt.expect_or_drop` with independently-verified pass/fail counts
- **Distributed synthetic data generation**: `spark.range()` + multi-seed `rand()` at 8M-row scale, vs. Faker for small dimension tables
- **Realistic debugging**: schema scoping bugs, silently-ignored invalid parameters, streaming-source append-only violations — all diagnosed via logs, `--debug` output, and independent SQL verification rather than guesswork

---

## Known Limitations / Next Steps

- **Single environment tested end-to-end** (`dev`); `prod` target is defined in `databricks.yml` but not yet exercised with genuinely isolated catalog/schema separation
- **No unit tests yet** for transformation logic (scaffolded `tests/` folder present but unused)
- **No CI/CD** — deployments are currently manual via local Databricks CLI (no Git integration due to local environment constraints)
- **Synthetic data only** — no real source system integration (API/JDBC/file-based ingestion)

---

## Author

Virendra Tambavekar — Data Engineer, Celebal Technologies
Built as a self-directed learning project to develop production-level fluency with Databricks Asset Bundles.
