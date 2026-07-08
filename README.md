# Health Insurance Patient Data — ETL & Analytics Pipeline

Idempotent, observable, validated Bronze/Silver/Gold pipeline for patient/claims/encounter
data, feeding Power BI. Same container image runs locally (Docker Compose) and on Azure
(Container Apps Job). See `plan.md`-equivalent design notes below and in the conversation
this was built from.

## Architecture

```
Landing (Blob/Azurite CSV)
   │  extract
   ▼
Bronze (bronze.*_raw, append-only, row_hash dedup)     ── idempotent re-ingest
   │  validate (Pydantic) + quarantine bad rows
   ▼
Silver (silver.patients [SCD2], silver.claims/encounters [natural-key upsert])
   │  conform to star schema
   ▼
Gold (gold.dim_patient, gold.dim_date, gold.fact_claims, gold.fact_encounters)
   │
   ▼
Power BI (gold.vw_* analytical views)
```

Every run is tracked in `audit.pipeline_runs` / `audit.pipeline_run_steps` (rows in/out/rejected,
duration, status) — this **is** the observability layer, and it also flows to Azure Application
Insights when `APPLICATIONINSIGHTS_CONNECTION_STRING` is set.

## Run it locally (fully working, no Azure account needed)

Requires Docker + Docker Compose.

```bash

# 1. Start infra (Postgres + Azurite blob emulator)
docker compose up -d postgres azurite

# 2. Seed the landing zone with synthetic patient/claims/encounter data
docker compose run --rm seed-landing

# 3. Run the ETL pipeline (Extract -> Bronze -> Silver -> Gold)
docker compose run --rm pipeline

# 4. Re-run it again to prove idempotency - row counts in gold won't double
docker compose run --rm pipeline

# 5. Inspect results
docker exec -it health_dw_postgres psql -U etl_user -d health_dw \
  -c "SELECT * FROM audit.pipeline_runs ORDER BY started_at DESC LIMIT 5;"
docker exec -it health_dw_postgres psql -U etl_user -d health_dw \
  -c "SELECT * FROM gold.vw_monthly_cost_trend LIMIT 12;"
```

Then point Power BI Desktop at `localhost:5432` / db `health_dw` (see `powerbi/README.md`).

### Run tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Deploy to Azure

```bash
az login
az group create -n rg-health-insurance-etl -l eastus

# 1. Provision infra (Storage/ADLS, PostgreSQL Flexible Server, Container Apps,
#    Key Vault, App Insights, Container Registry, Managed Identity)
az deployment group create \
  -g rg-health-insurance-etl \
  -f infra/main.bicep \
  -p infra/main.parameters.json \
  -p pgAdminPassword='<use a secure value / Key Vault reference>'

# 2. Build & push the pipeline image to the ACR created above
az acr build --registry <acrName from output> --image health-insurance-etl:latest .

# 3. The Container Apps Job is already wired to that image + a daily 02:00 UTC
#    cron trigger (infra/main.bicep). Trigger a manual run to verify:
az containerapp job start -n <etlJobName> -g rg-health-insurance-etl
```

The job authenticates to Storage and Key Vault via its **user-assigned managed identity** —
no secrets baked into the image. PostgreSQL admin password is stored in Key Vault and injected
as a Container Apps secret.

## Idempotency — how it's guaranteed at each layer

| Layer                      | Mechanism                                                                                            |
| -------------------------- | ---------------------------------------------------------------------------------------------------- |
| Bronze                     | `row_hash` (sha256 of raw row) is the primary key; `ON CONFLICT DO NOTHING`                          |
| Silver — patients          | SCD2: a new version is only opened if the **attribute hash** changed; identical re-ingest is a no-op |
| Silver — claims/encounters | Natural key (`claim_id`/`encounter_id`) primary key; `ON CONFLICT DO UPDATE` (upsert)                |
| Gold                       | Same upsert pattern from Silver; `dim_date` is `ON CONFLICT DO NOTHING`                              |
| Whole pipeline             | Safe to re-run for the same `batch_date`, or resume after a crash — never produces duplicate facts   |

## Validation & data quality gate

- Every row is validated against a Pydantic schema (`src/models.py`) before Silver: type checks,
  enum checks (gender, plan_type, claim_status), and business rules (DOB not in future,
  `approved_amount <= claim_amount`, discharge after admit).
- Rejected rows go to `audit.quarantine` with a human-readable reason — nothing silently disappears.
- If the rejection rate for any entity exceeds `MAX_QUARANTINE_RATE` (default 10%), the **pipeline
  fails the run** rather than loading suspect data into Gold — verified in this build: the
  synthetic dataset injects ~3% bad patient rows and ~2% bad claim rows, and the pipeline
  correctly quarantines exactly those rows (see `tests/test_validation.py`, all passing).

## Repo layout

```
src/            pipeline code (config, observability, models, validation, ingestion,
                transform_silver, transform_gold, pipeline.py = orchestrator)
sql/            schema.sql (DDL, idempotent) + gold_views.sql (Power BI-facing views)
data/           synthetic data generator + landing-zone seeder
infra/          main.bicep (Azure IaC) + parameters
powerbi/        DAX measures + report page guide
tests/          pytest unit tests (validation rules, NaN handling)
docker-compose.yml, Dockerfile   local + Azure-identical container run
```
