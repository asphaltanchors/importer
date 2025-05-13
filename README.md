# QuickBooks Data Pipeline with DBT

This project combines a DLT (Data Loading Tool) pipeline with DBT (Data Build Tool) transformations to process QuickBooks data.

## Components

1. **DLT Pipeline**: Extracts data from CSV files and loads it into a Postgres database
   - Customers
   - Items
   - Sales Receipts
   - Invoices

2. **DBT Pipeline**: Transforms the loaded data
   - `unique_customer_names`: Extracts distinct canonical company names from the customers table

## Setup

1. Install dependencies:
   ```
   uv pip install -r requirements.txt
   ```

2. Set up environment variables in a `.env` file:
   ```
   DROPBOX_PATH=/path/to/your/dropbox/folder
   DATABASE_URL=postgresql://user:password@host:port/dbname
   ```

3. Optionally, configure DBT environment variables:
   ```
   DBT_HOST=localhost
   DBT_USER=postgres
   DBT_PASSWORD=postgres
   DBT_PORT=5432
   DBT_DATABASE=postgres
   DBT_SCHEMA=public
   ```

## Running the Pipeline

Run the combined pipeline with:

```
python pipeline.py
```

This will:
1. Extract data from CSV files in the specified Dropbox folder and load it into the `qb_data` schema in Postgres
<!-- 2. Run the DBT transformations to create views in the `qb_analytics` schema -->

TODO: ADD SECTION ABOUT RUNNING DBT ON OWN. NO LONGER USES DLT integration
## Troubleshooting

If you encounter errors with the DBT step:

1. Check the terminal output for detailed error messages
2. Review the DBT logs in `logs/dbt.log`
3. Ensure your database connection details are correct in the `.env` file
4. Verify that the database user has the necessary permissions
5. Make sure the DBT project is properly configured in `dbt_project.yml`
6. Check that the database connection details are being properly passed to the DBT package in `pipeline.py`

## DBT Models

- `unique_customer_names`: Selects distinct canonical company names from the customers table

## Project Structure

```
.
├── .env                  # Environment variables (not in repo)
├── README.md             # This file
├── dbt_project.yml       # DBT project configuration
├── matcher.py            # Company name normalization logic
├── models/               # DBT models
│   ├── sources.yml       # DBT sources definition
│   └── unique_customer_names.sql  # DBT model for unique customer names
├── pipeline.py           # Main pipeline script
├── profiles.yml          # DBT connection profiles
└── requirements.txt      # Python dependencies
