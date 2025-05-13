# Data Quality Insurance (DQI) DBT Project

This directory contains the DBT models for the Data Quality Insurance project. The models are organized following DBT best practices into different layers.

## Project Structure

```
models/
├── staging/     # Minimal transformations, 1:1 with source tables
├── intermediate/ # Business transformations, joining, and calculations
└── mart/        # Business-facing models for end-users
```

## Model Layers

### Staging (`staging/`)
- Light transformations 
- One-to-one with source tables
- Cleaning, type casting, standardizing names
- Materialized as views by default

### Intermediate (`intermediate/`)
- Transformations that join data but aren't ready for consumption
- Business logic applications
- Aggregations and calculations
- Materialized as views by default

### Mart (`mart/`)
- Business-entity focused models ready for consumption
- Prefixed with `fct_` (fact/events) or `dim_` (dimensions)
- Materialized as tables by default

## Naming Conventions

- `base_*`: Source data with minimal cleaning
- `stg_*`: Staged data with column standardization
- `int_*`: Intermediate transformation
- `fct_*`: Fact tables (events, transactions)
- `dim_*`: Dimension tables (people, products, etc.)

## Usage

Run all models:
```
dbt run
```

Run tests:
```
dbt test
```

Generate documentation:
```
dbt docs generate
dbt docs serve
```

## Conventions

- Each model directory contains a YAML file documenting the models
- Use present tense column names (e.g., `is_tax_exempt` not `was_tax_exempt`)
- Use simple, consistent test patterns