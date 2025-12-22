# Renewable Asset Performance Pipeline

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade analytics engineering pipeline for weather-adjusted renewable energy asset performance analysis. This project demonstrates modern data engineering practices using Dagster orchestration, dlt ingestion, dbt transformations, and Polars for efficient data processing.

---

## **Table of Contents**

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [KPI & Metrics](#kpi--metrics)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Development](#development)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)

---

## **Project Overview**

This pipeline analyzes the performance of 5-10 renewable energy assets (wind and solar) by correlating hourly weather data with generation output. The system computes capacity factors, weather-normalized performance metrics, and rolling correlations to provide actionable insights for asset management.

**Key Features:**

- **Orchestration**: Dagster for modular, testable data workflows
- **Ingestion**: dlt for incremental loading from Parquet to DuckDB
- **Transformation**: dbt for dimensional modeling and business logic
- **Processing**: Polars for high-performance data manipulation
- **Storage**: DuckDB as an embedded analytical warehouse

---

## **Architecture**

```
┌─────────────────┐
│  Mock Data Gen  │
│   (Polars)      │
└────────┬────────┘
         │ Parquet Files
         ▼
┌─────────────────┐
│   dlt Loader    │
│  (Incremental)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    DuckDB       │
│   (Warehouse)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   dbt Models    │
│  (Transform)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Analytics      │
│  Marts          │
└─────────────────┘

       ▲
       │
  Dagster
  Orchestration
```

**Data Flow:**

1. **Generation**: Mock data generators create realistic hourly weather and generation data
2. **Ingestion**: dlt pipelines incrementally load Parquet files into DuckDB
3. **Staging**: dbt staging models apply initial transformations and data quality checks
4. **Intermediate**: Daily aggregations and asset-weather joins
5. **Marts**: Final analytical tables with KPIs and performance metrics

---

## **Data Model**

### **Source Tables**

**`weather`**
- `timestamp` (TIMESTAMP)
- `asset_id` (VARCHAR)
- `wind_speed_mps` (DOUBLE)
- `wind_direction_deg` (DOUBLE)
- `ghi` (DOUBLE) — Global Horizontal Irradiance
- `temperature_c` (DOUBLE)
- `pressure_hpa` (DOUBLE)
- `relative_humidity` (DOUBLE)

**`generation`**
- `timestamp` (TIMESTAMP)
- `asset_id` (VARCHAR)
- `gross_generation_mwh` (DOUBLE)
- `net_generation_mwh` (DOUBLE)
- `curtailment_mwh` (DOUBLE)
- `availability_pct` (DOUBLE)
- `asset_capacity_mw` (DOUBLE)

### **Mart Tables**

**`mart_asset_performance_daily`**
- Daily capacity factors
- Generation summaries
- Availability metrics

**`mart_asset_weather_performance`**
- Weather-normalized performance
- Correlation coefficients
- Performance scoring

---

## **KPI & Metrics**

### **Capacity Factor**
```
Capacity Factor = Net Generation (MWh) / (Asset Capacity (MW) × Hours)
```

### **Weather-Adjusted Expected Generation**
Regression-based model predicting expected generation from weather conditions.

### **Rolling Correlations**
7-day and 30-day Pearson correlations between:
- Wind speed ↔ Wind generation
- GHI ↔ Solar generation

### **Performance Score**
Normalized score (0-100) comparing actual vs. expected generation.

---

## **Getting Started**

### **Prerequisites**

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

### **Installation**

1. Clone the repository:
   ```bash
   cd /Users/cdcoonce/Documents/GitHub/Weather_Adjusted_Generation_Analytics
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. Copy environment configuration:
   ```bash
   cp .env.example .env
   ```

4. Create data directories:
   ```bash
   mkdir -p data/raw/weather data/raw/generation data/intermediate data/processed
   ```

---

## **Usage**

### **1. Generate Mock Data**

```bash
uv run python weather_adjusted_generation_analytics/mock_data/generate_weather.py
uv run python weather_adjusted_generation_analytics/mock_data/generate_generation.py
```

This creates 2 years of hourly data for 10 assets as Parquet files.

### **2. Run dlt Ingestion**

```bash
uv run python weather_adjusted_generation_analytics/loaders/dlt_pipeline.py
```

Loads Parquet data incrementally into DuckDB.

### **3. Run dbt Transformations**

```bash
cd dbt/renewable_dbt
uv run dbt deps
uv run dbt build
```

### **4. Start Dagster**

```bash
cd dags/dagster_project
uv run dagster dev
```

Access the Dagster UI at http://localhost:3000

### **5. Explore Analysis Notebooks**

```bash
uv run jupyter lab
```

Open notebooks in `notebooks/` for EDA and correlation analysis.

---

## **Project Structure**

```
renewable_performance_pipeline/
├── README.md                       # This file
├── COPILOT_INSTRUCTIONS.md         # Project requirements
├── pyproject.toml                  # Dependencies and tooling
├── .env.example                    # Environment template
├── .gitignore
│
├── data/                           # Data storage (gitignored)
│   ├── raw/
│   │   ├── generation/
│   │   └── weather/
│   ├── intermediate/
│   └── processed/
│
├── dags/                           # Dagster orchestration
│   └── dagster_project/
│       ├── workspace.yaml
│       ├── dagster.yaml
│       ├── assets/                 # Data assets
│       ├── jobs/                   # Job definitions
│       ├── schedules/              # Scheduled runs
│       ├── sensors/                # Event-driven triggers
│       └── resources/              # Shared resources
│
├── dbt/                            # dbt transformations
│   └── renewable_dbt/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/            # Source transformations
│       │   ├── intermediate/       # Business logic
│       │   ├── marts/              # Analytics tables
│       │   └── sources.yml
│       ├── macros/                 # Reusable SQL
│       ├── tests/                  # Data quality tests
│       └── profiles/               # Connection profiles
│
├── weather_adjusted_generation_analytics/  # Python source code
│   ├── config/                           # Configuration management
│   ├── mock_data/                        # Data generators
│   ├── loaders/                          # dlt pipelines
│   ├── pipelines/                        # Processing logic
│   └── utils/                            # Shared utilities
│
└── notebooks/                      # Jupyter analysis
    ├── eda_weather.ipynb
    ├── eda_generation.ipynb
    └── correlation_summary.ipynb
```

---

## **Development**

### **Code Quality**

This project enforces strict code quality standards:

- **Type hints**: All functions must include complete type annotations
- **Docstrings**: NumPy-style documentation for all public interfaces
- **Linting**: Ruff for fast Python linting
- **Type checking**: mypy for static type analysis
- **Testing**: pytest for unit and integration tests

Run quality checks:

```bash
uv run ruff check .
uv run mypy weather_adjusted_generation_analytics/
uv run pytest
```

### **Pre-commit Hooks**

Install pre-commit hooks:

```bash
uv run pre-commit install
```

---

## **Future Enhancements**

- [ ] Add real-time streaming ingestion (Kafka/Kinesis)
- [ ] Implement ML-based anomaly detection
- [ ] Add weather forecast integration (NOAA, OpenWeatherMap)
- [ ] Build Grafana/Streamlit dashboard
- [ ] Expand to support additional asset types (hydro, battery storage)
- [ ] Add geospatial analysis for regional patterns
- [ ] Implement data quality monitoring (Great Expectations)
- [ ] Add CI/CD pipeline (GitHub Actions)

---

## **Contributing**

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Ensure code quality checks pass
4. Commit your changes with clear messages
5. Push to your branch
6. Open a Pull Request

---

## **License**

This project is licensed under the MIT License - see the LICENSE file for details.

---

## **Contact**

For questions or feedback, please open an issue in the GitHub repository.

**Built with ❤️ using Dagster, dbt, Polars, and DuckDB**
