# WAGA Dagster gRPC code-location image.
#
# Runs on the home server (Linux x86_64, Intel i5-3230M, NO AVX2) behind a
# Dagster OSS webserver/daemon using DockerRunLauncher: every run is a fresh
# container from THIS image (multiprocess executor — all steps of one run
# share that container's filesystem; nothing persists between runs). Secrets
# (Snowflake creds, CLOUDFLARE_API_TOKEN/ACCOUNT_ID, etc.) arrive as env vars
# injected by the run launcher at container-start time — this image never
# bakes in a secret (see .dockerignore: .env/.env.* excluded, dbt parse below
# uses dummy build-only creds).
#
# Style/comments follow the Oura-Pipeline reference image (PR #10).
#
# Node.js (>=22) for wrangler, copied from the official Node image below
# (multi-stage copy, not apt/nodesource) — keeps this layer small and avoids
# adding a second package manager's trust chain to the final image.
FROM node:22-bookworm-slim AS node

FROM python:3.12-slim

# Only the node runtime + global-modules dir are copied from the node stage;
# npm/npx are symlinked back in because the copied node_modules/npm bin
# scripts expect them alongside node.
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

# --- uv (pinned) ---
COPY --from=ghcr.io/astral-sh/uv:0.12.13 /uv /usr/local/bin/uv

# WORKDIR is the repo root, not a subdirectory: DBT_PROJECT_DIR
# ("dbt/renewable_dbt") and the dashboard export/dist directories are all
# CWD-relative, so the Dagster gRPC server must be started from here.
WORKDIR /opt/dagster/app

# --- wrangler, installed globally and pinned at build time ---
# A pinned global install (not `npx --yes wrangler` at run time) means a run
# never needs to reach the npm registry — cockpit/cloudflare.py resolves
# `wrangler` off PATH via shutil.which() first and only falls back to
# `npx --yes wrangler` when no global install is found (the launchd host).
# WRANGLER_SEND_METRICS=false: no telemetry calls from an unattended run.
RUN npm install -g wrangler@4.131.2
ENV WRANGLER_SEND_METRICS=false

# --- Python deps: two-layer pattern so dependency layers cache independently
# of application code changes (`--no-install-project` installs deps only). ---
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra deploy --no-install-project
COPY . .
RUN uv sync --frozen --no-dev --extra deploy
ENV PATH="/opt/dagster/app/.venv/bin:${PATH}"

# --- dbt manifest ---
# `dbt parse` (not `dbt deps` alone) needs the packages installed first, and
# needs *a* target to parse against — WAGA_SNOWFLAKE_* are dummy build-only
# values (never real creds; the profile only needs them to be non-empty, no
# connection is opened by `parse`). This bakes dagster-dbt's manifest.json
# into the image so `waga_dbt_assets` exists at container start without a
# Snowflake round-trip on every gRPC server boot.
RUN dbt deps --project-dir dbt/renewable_dbt --profiles-dir dbt/renewable_dbt/profiles
RUN WAGA_SNOWFLAKE_ACCOUNT=build WAGA_SNOWFLAKE_USER=build WAGA_SNOWFLAKE_ROLE=build WAGA_SNOWFLAKE_WAREHOUSE=build \
    dbt parse --project-dir dbt/renewable_dbt --profiles-dir dbt/renewable_dbt/profiles

# DAGSTER_HOME here is the in-container instance dir the gRPC server process
# itself may touch (compute log staging, etc.) — the daemon's actual run/event
# storage lives in Postgres (dagster-postgres, see the `deploy` extra), not in
# this ephemeral container's filesystem.
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p "${DAGSTER_HOME}" /opt/dagster/compute_logs

EXPOSE 4001
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4001", "-m", "weather_analytics.definitions"]
