# Snowflake Infrastructure & Dagster Cloud Deployment

> Design spec for completing the WAGA transition: Snowflake bootstrap, key-pair auth wiring, and Dagster Cloud deployment pipelines.

## Context

The WAGA pipeline code is fully migrated from DuckDB to Snowflake (`src/weather_analytics/`). What remains is standing up the infrastructure and deployment pipeline so it actually runs. This spec covers everything needed to go from "code exists" to "pipeline running on Dagster Cloud against Snowflake."

The Oura Pipeline repo (`cdcoonce/Oura-Pipeline`) is the reference implementation — same owner, same Dagster Cloud org (`charles-likes-data.dagster.plus`), same Snowflake account. This spec mirrors those patterns with WAGA-specific naming.

## Deliverables

### 1. Snowflake Bootstrap SQL

**File:** `docs/snowflake/bootstrap.sql`

Run once by ACCOUNTADMIN in the Snowflake console. Creates:

- **Role:** `WAGA_TRANSFORM`
- **Warehouse:** `WAGA_WH` (XSMALL, auto-suspend 60s, auto-resume)
- **Database:** `WAGA`
- **Schemas:** `RAW`, `STAGING`, `MARTS`, `ANALYTICS`
- **User:** `WAGA_PIPELINE` (service account, key-pair auth, no password)
- **Grants:** Full read/write on all WAGA schemas, CREATE TABLE/VIEW/SCHEMA, warehouse usage

The script includes a placeholder comment for `ALTER USER ... SET RSA_PUBLIC_KEY` after key generation.

### 2. Key-Pair Auth Wiring

**Key generation instructions** included in `bootstrap.sql` as comments:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/waga_rsa_key.p8 -nocrypt
openssl rsa -in ~/.snowflake/waga_rsa_key.p8 -pubout -out ~/.snowflake/waga_rsa_key.pub
base64 -i ~/.snowflake/waga_rsa_key.p8 | tr -d '\n'
```

**Temp key file function** in `src/weather_analytics/assets/dbt_assets.py`:

Mirrors the Oura `_ensure_key_file()` pattern:

- Reads `WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64` env var
- Decodes base64 to a temporary `.p8` file in a secure temp directory
- Sets `WAGA_SNOWFLAKE_PRIVATE_KEY_PATH` env var for dbt-snowflake
- Registers `atexit` cleanup to delete the temp file
- Called inside the `waga_dbt_assets` function before `dbt.cli()`
- Idempotent — skips if `WAGA_SNOWFLAKE_PRIVATE_KEY_PATH` is already set and file exists

### 3. Dagster Cloud Deploy Workflow

**File:** `.github/workflows/deploy.yml`

Triggered on push to `main`. Three stages:

1. **Test** — `uv sync --extra dev && uv run pytest -m unit -q`
2. **dbt manifest** — Inject `WAGA_SNOWFLAKE_*` from GitHub Secrets, run `dbt deps && dbt parse --profiles-dir .` in `dbt/renewable_dbt/`
3. **Deploy** — `dagster-io/dagster-cloud-action` serverless prod deploy

Environment:

- `DAGSTER_CLOUD_URL`: `https://charles-likes-data.dagster.plus`
- `DAGSTER_CLOUD_API_TOKEN`: from secrets
- `DAGSTER_CLOUD_FILE`: `dagster_cloud.yaml`
- `PYTHON_VERSION`: `3.12`

### 4. Branch Deploy Workflow

**File:** `.github/workflows/branch_deployments.yml`

Triggered on PR open/synchronize/reopen/close. Same test + manifest generation, then `serverless_branch_deploy` for ephemeral preview environments.

### 5. GitHub Secrets Required

| Secret                              | Value                          |
| ----------------------------------- | ------------------------------ |
| `DAGSTER_CLOUD_API_TOKEN`           | Dagster Cloud API token        |
| `WAGA_SNOWFLAKE_ACCOUNT`            | Snowflake account identifier   |
| `WAGA_SNOWFLAKE_USER`               | `WAGA_PIPELINE`                |
| `WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64` | Base64-encoded PEM private key |
| `WAGA_SNOWFLAKE_WAREHOUSE`          | `WAGA_WH`                      |
| `WAGA_SNOWFLAKE_DATABASE`           | `WAGA`                         |
| `WAGA_SNOWFLAKE_ROLE`               | `WAGA_TRANSFORM`               |

Same variables must be configured in Dagster Cloud UI as environment variables for runtime access.

### 6. Cleanup

- **`.env.example`** — Remove all DuckDB-era variables (lines 1-30). Keep only `WAGA_*` section.
- **`data/warehouse.duckdb`** — Delete from disk (already gitignored).
- **`.gitignore`** — Verify `data/` and `*.duckdb` are ignored.

## What This Spec Does NOT Cover

- CI integration tests against Snowflake (deferred — get pipeline running first)
- Real data sources (mock data only for first run validation)
- Dashboard/BI layer
- Alerting beyond Dagster asset checks

## Validation

After deployment, materialize assets in order from the Dagster Cloud UI:

1. `waga_weather_ingestion` + `waga_generation_ingestion` — verify data in `WAGA.RAW`
2. `waga_dbt_assets` — verify models in `WAGA.STAGING` and `WAGA.MARTS`
3. `waga_correlation_analysis` — verify output in `WAGA.ANALYTICS`

Once confirmed, enable the three schedules (daily ingestion, daily dbt, weekly analytics).
