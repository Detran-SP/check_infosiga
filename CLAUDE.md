# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python Shiny web application for automated data quality validation of Infosiga-SP (São Paulo traffic accident data). Users upload a ZIP file containing CSV data, and the application generates interactive HTML validation reports for three data tables: sinistros (accidents), veículos (vehicles), and pessoas (people).

## Commands

``` bash
# Install dependencies
uv sync

# Run the Shiny app (available at http://127.0.0.1:8000)
uv run shiny run app.py
```

## Architecture

The application follows a layered architecture:

-   **app.py**: Shiny UI and server logic. Handles file upload, coordinates data processing and validation, renders tabbed HTML reports. Uses reactive calculations (`@reactive.calc`) to chain data loading → validation.

-   **data_processing.py**: Contains `read_infosiga()` function that extracts CSVs from ZIP and parses them into pandas DataFrames with proper types. CSV files use semicolon separator and latin1 encoding with Brazilian date/number formats (comma as decimal separator, dd/mm/yyyy dates).

-   **schemas.py**: Defines pointblank Schema objects for each table's expected column types, and `create_valid_data()` which returns dictionaries of allowed values for categorical columns (e.g., valid municipality names, vehicle types, severity levels).

-   **validation.py**: Contains `create_*_agent()` functions that build pointblank validation pipelines. Each function creates a `pb.Validate` object with chained validation rules, runs `.interrogate()`, and returns HTML report string. Validation rules include schema matching, value range checks, set membership, null checks, and conditional validations (e.g., death date required when severity is FATAL).

## Key Technical Details

-   Uses `pointblank` library for data validation with Portuguese language reports (`lang="pt"`, `locale="pt"`)
-   Municipality list loaded from `data/municipios.json`
-   `data_release` date in app.py controls the expected date range for validation (currently hardcoded)
-   Latitude/longitude bounds validate against São Paulo state geographic boundaries
-   Three table types have distinct schemas and validation rules defined separately