# Getting Started with Renewable Performance Pipeline

This guide will walk you through setting up and running the complete renewable energy analytics pipeline.

## Prerequisites

- Python 3.12 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- 5-10 GB of free disk space (for data and DuckDB)

## Quick Start

### 1. Initial Setup

```bash
# Run the setup script
./setup.sh
```

This script will:
- Install all Python dependencies via uv
- Create the `.env` configuration file
- Create required data directories
- Install dbt packages

### 2. Generate Mock Data

Generate 2 years of hourly weather and generation data for 10 assets:

```bash
# Generate weather data (~730 daily files)
uv run python src/mock_data/generate_weather.py

# Generate generation data (~730 daily files)
uv run python src/mock_data/generate_generation.py
```

**Expected output**: ~17,520 hours × 10 assets = 175,200 rows per dataset

### 3. Ingest Data with dlt

Load the Parquet files into DuckDB:

```bash
uv run python src/loaders/dlt_pipeline.py
```

This will:
- Create the DuckDB database at `data/warehouse.duckdb`
- Load weather data into `renewable_energy.weather` table
- Load generation data into `renewable_energy.generation` table
- Display verification statistics

### 4. Run dbt Transformations

Transform raw data into analytics-ready marts:

```bash
cd dbt/renewable_dbt

# Install dbt dependencies (if not done by setup script)
uv run dbt deps

# Run all models and tests
uv run dbt build

# Or run models only
uv run dbt run

# Or run tests only
uv run dbt test
```

**Models created**:
- `staging.stg_weather` - Cleaned weather data with time features
- `staging.stg_generation` - Cleaned generation data with performance metrics
- `intermediate.int_weather_daily` - Daily weather aggregations
- `intermediate.int_generation_daily` - Daily generation aggregations
- `intermediate.int_asset_weather_join` - Hourly joined data
- `marts.mart_asset_performance_daily` - Daily performance KPIs
- `marts.mart_asset_weather_performance` - Weather-adjusted performance with correlations

### 5. Start Dagster Orchestration

Launch the Dagster UI to orchestrate the pipeline:

```bash
cd dags/dagster_project
uv run dagster dev
```

Access the UI at **http://localhost:3000**

**Available assets**:
- `weather_data` - Weather ingestion asset
- `generation_data` - Generation ingestion asset
- `weather_generation_correlation` - Correlation analysis asset

**Available jobs**:
- `daily_ingestion_job` - Run weather and generation ingestion
- `daily_dbt_job` - Run dbt transformations
- `correlation_job` - Run correlation analysis

**Schedules**:
- Daily ingestion at 6:00 AM
- Daily dbt run at 7:00 AM
- Weekly performance summary every Monday at 8:00 AM

### 6. Explore Data with Jupyter

Launch Jupyter to run analysis notebooks:

```bash
uv run jupyter lab
```

**Available notebooks**:
- `notebooks/eda_weather.ipynb` - Weather data exploration
- `notebooks/eda_generation.ipynb` - Generation data exploration
- `notebooks/correlation_summary.ipynb` - Weather-generation correlation analysis

## Directory Structure

```
renewable_performance_pipeline/
├── data/
│   ├── raw/
│   │   ├── weather/          # Weather parquet files (730 files)
│   │   └── generation/       # Generation parquet files (730 files)
│   └── warehouse.duckdb      # DuckDB database file
├── dbt/renewable_dbt/        # dbt project
│   ├── models/               # SQL models
│   └── target/               # Compiled SQL and results
├── dags/dagster_project/     # Dagster orchestration
│   ├── assets/               # Data assets
│   ├── jobs/                 # Job definitions
│   ├── schedules/            # Schedules
│   └── sensors/              # Sensors
├── src/                      # Python source code
│   ├── config/               # Configuration
│   ├── loaders/              # dlt pipelines
│   ├── mock_data/            # Data generators
│   └── utils/                # Utilities
└── notebooks/                # Jupyter notebooks
```

## Common Workflows

### Daily Operations

1. **New data arrives** → Sensor detects files → Triggers ingestion job
2. **Ingestion completes** → dbt schedule runs transformations
3. **Transformations complete** → Marts are updated
4. **Weekly Monday** → Correlation analysis runs

### Manual Execution

```bash
# Run specific dbt models
cd dbt/renewable_dbt
uv run dbt run --select stg_weather
uv run dbt run --select marts

# Run dlt for specific files
uv run python src/loaders/weather_loader.py

# Run correlation analysis
uv run python -c "from src.loaders import verify_ingestion; verify_ingestion()"
```

### Query DuckDB Directly

```bash
# Install DuckDB CLI (if not already installed)
brew install duckdb  # macOS
# or
apt-get install duckdb  # Linux

# Query the database
duckdb data/warehouse.duckdb

# Example queries:
SELECT COUNT(*) FROM renewable_energy.weather;
SELECT * FROM marts.mart_asset_performance_daily LIMIT 10;
```

## Troubleshooting

### Issue: Import errors when running Python scripts

**Solution**: Ensure you're using `uv run` to execute scripts:
```bash
uv run python src/mock_data/generate_weather.py
```

### Issue: dbt cannot find profiles

**Solution**: Ensure profiles are in the correct location:
```bash
cd dbt/renewable_dbt
uv run dbt debug
```

### Issue: Dagster assets not appearing

**Solution**: Restart Dagster dev server:
```bash
cd dags/dagster_project
uv run dagster dev
```

### Issue: DuckDB file locked

**Solution**: Close any open connections and retry:
```bash
rm data/warehouse.duckdb.wal
```

## Next Steps

1. **Customize Configuration**: Edit `.env` to adjust data paths, date ranges, asset count
2. **Add Real Data Sources**: Replace mock generators with real data connectors
3. **Extend Models**: Add custom dbt models for specific analysis needs
4. **Create Dashboards**: Connect BI tools (Grafana, Streamlit) to DuckDB
5. **Deploy to Production**: Set up scheduled runs on a server

## Performance Tips

- **Parquet Partitioning**: Daily files enable incremental loading
- **DuckDB Performance**: Increase `DUCKDB_MEMORY_LIMIT` in `.env` for large datasets
- **dbt Compilation**: Use `dbt compile` to check SQL before running
- **Polars Operations**: Leverage lazy evaluation for large dataframes

## Additional Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [dbt Documentation](https://docs.getdbt.com/)
- [dlt Documentation](https://dlthub.com/docs/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Polars Documentation](https://pola-rs.github.io/polars/)

## Support

For issues or questions:
1. Check this guide's troubleshooting section
2. Review the README.md
3. Open an issue in the GitHub repository
