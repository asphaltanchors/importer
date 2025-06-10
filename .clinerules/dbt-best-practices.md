## Brief overview
This set of guidelines focuses on DBT best practices for the DQI project, which processes QuickBooks data through a series of transformations. These rules ensure maintainable, efficient, and well-structured data models.

## Model organization
- Follow the standard DBT layering pattern: sources → staging → intermediate → mart (fact/dimension)
- Keep each model focused on a single, clear purpose
- Break down complex transformations into smaller, manageable models
- When models become too large or complex, refactor them into multiple smaller models

## Coding standards
- Use clear, descriptive model and column names that reflect their purpose
- Include detailed comments explaining complex transformations
- Document models with descriptions in YAML files
- Use CTEs to break down complex logic into readable chunks
- Prefer SQL functions over complex expressions when possible
- Avoid common SQL syntax errors like trailing commas in SELECT statements
- Keep schema YAML files in sync with model changes to ensure documentation accuracy

## Testing and validation
- Verify data integrity between model layers (e.g., count checks between source and final models)
- Implement appropriate tests for each model (unique, not_null, relationships)
- When making changes, validate that all orders and data points are preserved through the transformation pipeline

## Performance considerations
- Avoid overly complex aggregations that might filter out data unintentionally
- Be mindful of join conditions to prevent data loss or duplication
- Consider materialization strategies based on model usage patterns
- Use incremental models for large tables when appropriate
- Use window functions (e.g., ROW_NUMBER()) to deduplicate data when necessary
- Consider the impact of join keys on row counts and performance

## Data integrity
- Ensure one row per business entity in fact tables by properly handling duplicates
- Choose appropriate join keys (business keys vs. system IDs) based on data requirements
- Add uniqueness tests on business key columns to catch unexpected duplicates
- When joining multiple sources, verify that join conditions don't create Cartesian products

## Development workflow
- When in doubt about a model's structure or complexity, refactor rather than extend
- Test queries against the MCP server to validate data transformations
- Compare record counts between source and target models to ensure data completeness
- Document any assumptions or business rules implemented in the models
