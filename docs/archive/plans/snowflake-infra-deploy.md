# Plan: Snowflake Infrastructure & Dagster Cloud Deploy

> Source spec: `docs/superpowers/specs/2026-04-05-snowflake-infra-deploy-design.md`
> Reference implementation: `cdcoonce/Oura-Pipeline`

## Architectural decisions

- **Dagster Cloud action version:** `v1.12.19` (matching Oura Pipeline)
- **Python version in deploy:** `3.12` (WAGA uses 3.12, Oura uses 3.10)
- **dbt manifest:** Generated in CI deploy step via `dbt parse`, not committed to git
- **Key-pair auth:** Base64 PEM in env var, temp `.p8` file at runtime via `_ensure_key_file()`
- **dbt project loading:** Use `DbtProject` class (like Oura) instead of raw `Path` to manifest — handles `prepare_if_dev()` for local dev
- **Env var naming:** `WAGA_SNOWFLAKE_*` (differs from Oura's `SNOWFLAKE_*`)
- **Deploy concurrency:** Cancel in-progress deploys to same branch
- **Existing CI:** `ci.yml` (lint + unit tests) stays unchanged. Deploy workflows are additive.

---

## Phase 1: Snowflake Bootstrap SQL + Cleanup

### What to build

Create `docs/snowflake/bootstrap.sql` with all Snowflake infrastructure DDL. Clean up `.env.example` to remove DuckDB-era variables. Remove `data/warehouse.duckdb` from disk.

### Acceptance criteria

- [ ] `docs/snowflake/bootstrap.sql` exists with role, warehouse, database, schemas, user, and grants
- [ ] SQL includes key generation instructions as comments
- [ ] SQL includes `ALTER USER ... SET RSA_PUBLIC_KEY` placeholder
- [ ] `.env.example` contains only `WAGA_*` variables (DuckDB section removed)
- [ ] `data/warehouse.duckdb` deleted from disk
- [ ] `.gitignore` includes `*.duckdb`

---

## Phase 2: dbt_assets.py — DbtProject + \_ensure_key_file()

### What to build

Rewrite `src/weather_analytics/assets/dbt_assets.py` to use `DbtProject` (like Oura) instead of raw `Path` to manifest. Add `_ensure_key_file()` that decodes `WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64` to a temp `.p8` file and sets `WAGA_SNOWFLAKE_PRIVATE_KEY_PATH`.

### Acceptance criteria

- [ ] `dbt_assets.py` uses `DbtProject(project_dir=..., profiles_dir=...)` and `.manifest_path`
- [ ] `dbt_assets.py` calls `dbt_project.prepare_if_dev()` for local development
- [ ] `_ensure_key_file()` decodes base64 env var to temp `.p8` file with `0o600` permissions
- [ ] `_ensure_key_file()` sets `WAGA_SNOWFLAKE_PRIVATE_KEY_PATH` env var
- [ ] `_ensure_key_file()` registers `atexit` cleanup
- [ ] `_ensure_key_file()` is idempotent (skips if path already set)
- [ ] `_ensure_key_file()` called inside `waga_dbt_assets` before `dbt.cli()`
- [ ] `definitions.py` updated if imports change
- [ ] Unit test: `_ensure_key_file()` creates file and sets env var
- [ ] Unit test: `_ensure_key_file()` is idempotent on second call
- [ ] Existing dbt_assets tests still pass (or updated for DbtProject pattern)

---

## Phase 3: Deploy Workflows

### What to build

Create `.github/workflows/deploy.yml` (prod) and `.github/workflows/branch_deployments.yml` (PRs) mirroring the Oura Pipeline pattern. Both include test, dbt manifest generation, and Dagster Cloud serverless deploy.

### Acceptance criteria

- [ ] `.github/workflows/deploy.yml` triggers on push to `main`
- [ ] `.github/workflows/branch_deployments.yml` triggers on PR open/synchronize/reopen/close
- [ ] Both workflows run unit tests before deploying
- [ ] Both workflows generate dbt manifest: `dbt deps && dbt parse --profiles-dir .` in `dbt/renewable_dbt/`
- [ ] dbt manifest step injects `WAGA_SNOWFLAKE_*` from GitHub Secrets
- [ ] Prod deploy uses `dagster-io/dagster-cloud-action/actions/serverless_prod_deploy@v1.12.19`
- [ ] Branch deploy uses `dagster-io/dagster-cloud-action/actions/serverless_branch_deploy@v1.12.19`
- [ ] Both workflows use `DAGSTER_CLOUD_URL: https://charles-likes-data.dagster.plus`
- [ ] Both workflows use `PYTHON_VERSION: '3.12'`
- [ ] Both workflows include PEX deploy path with dbt manifest generation
- [ ] Both workflows include Docker deploy path for serverless
- [ ] Deploy concurrency configured (cancel in-progress to same branch)
- [ ] `ORGANIZATION_ID` secret used in Docker deploy step

---

## Phase 4: dagster_cloud.yaml + Final Wiring

### What to build

Verify/update `dagster_cloud.yaml` to ensure the build step works with the deploy workflow. Update CLAUDE.md if needed. Verify the full CI + deploy pipeline configuration is consistent.

### Acceptance criteria

- [ ] `dagster_cloud.yaml` build commands work with the PEX deploy path
- [ ] `dagster_cloud.yaml` `module_name` matches what `definitions.py` exports
- [ ] CLAUDE.md updated with deploy workflow information
- [ ] All unit tests pass: `uv run pytest -m unit -q`
- [ ] `ruff check src/weather_analytics/` clean
