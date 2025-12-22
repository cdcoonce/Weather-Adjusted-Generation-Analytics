#!/usr/bin/env bash

# Setup script for Renewable Performance Pipeline
# This script initializes the project and prepares it for use

set -e

echo "========================================"
echo "Renewable Performance Pipeline Setup"
echo "========================================"
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed"
    echo "Please install uv: https://github.com/astral-sh/uv"
    exit 1
fi

echo "✓ uv is installed"
echo ""

# Install dependencies
echo "Installing Python dependencies..."
uv sync
echo "✓ Dependencies installed"
echo ""

# Create .env from .env.example if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "✓ .env file created"
    echo "  Please review and update .env with your settings"
else
    echo "✓ .env file already exists"
fi
echo ""

# Create required directories
echo "Creating data directories..."
mkdir -p data/raw/weather
mkdir -p data/raw/generation
mkdir -p data/intermediate
mkdir -p data/processed
mkdir -p dags/dagster_project/storage
mkdir -p dags/dagster_project/logs
echo "✓ Data directories created"
echo ""

# Install dbt packages
echo "Installing dbt packages..."
cd dbt/renewable_dbt
uv run dbt deps
cd ../..
echo "✓ dbt packages installed"
echo ""

echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Generate mock data:"
echo "   uv run python src/mock_data/generate_weather.py"
echo "   uv run python src/mock_data/generate_generation.py"
echo ""
echo "2. Run dlt ingestion:"
echo "   uv run python src/loaders/dlt_pipeline.py"
echo ""
echo "3. Run dbt transformations:"
echo "   cd dbt/renewable_dbt"
echo "   uv run dbt build"
echo ""
echo "4. Start Dagster UI:"
echo "   cd dags/dagster_project"
echo "   uv run dagster dev"
echo ""
echo "For more information, see README.md"
