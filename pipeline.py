# pipeline.py

import os
import glob
import csv
from datetime import datetime

import dlt
from dotenv import load_dotenv
from matcher import normalize_company_name

# 0) Load environment
load_dotenv()
DROPBOX_PATH = os.environ["DROPBOX_PATH"]

# 1) Define your source
@dlt.source
def qb_source():
    # customers resource
    @dlt.resource(write_disposition="append", name="customers")
    def extract_customers():
        pattern = os.path.join(DROPBOX_PATH, "Customer_*.csv")
        for f in glob.glob(pattern):
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield {
                        **row,
                        "load_date": datetime.utcnow().date().isoformat(),
                        "canonical_name": normalize_company_name(row.get("Company Name", ""))
                    }

    # items resource
    @dlt.resource(write_disposition="append", name="items")
    def extract_items():
        pattern = os.path.join(DROPBOX_PATH, "Item_*.csv")
        for f in glob.glob(pattern):
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { **row, "load_date": datetime.utcnow().date().isoformat() }

    # sales_receipts
    @dlt.resource(write_disposition="append", name="sales_receipts")
    def extract_sales_receipts():
        pattern = os.path.join(DROPBOX_PATH, "Sales*.csv")
        for f in glob.glob(pattern):
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { **row, "load_date": datetime.utcnow().date().isoformat() }

    # invoices
    @dlt.resource(write_disposition="append", name="invoices")
    def extract_invoices():
        pattern = os.path.join(DROPBOX_PATH, "Invoice_*.csv")
        for f in glob.glob(pattern):
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { **row, "load_date": datetime.utcnow().date().isoformat() }

    # **Return** your resource functions in a list
    return [
        extract_customers,
        extract_items,
        extract_sales_receipts,
        extract_invoices
    ]

# 3) Run it
if __name__ == "__main__":
    # 1. Run DLT pipeline to load data
    load_pipeline = dlt.pipeline(
        pipeline_name="dqi_pipeline",
        destination="postgres",
        dataset_name="raw",
    )
    load_info = load_pipeline.run(qb_source())
    print("DLT pipeline complete:", load_info)