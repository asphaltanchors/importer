# Data Pipeline PRD

---

## 1. Purpose & Scope  
**Goal:** Build a lean, Docker-based DLT pipeline that:  
- Ingests four QuickBooks CSVs (customers, items, sales receipts, invoices) from a synced Dropbox folder  
- Handles full-history and daily incremental loads  
- Tracks daily item inventory snapshots  
- Applies an existing Python name-matching hook  
- Is trivial to deploy, configure, and extend (no Meltano)

**Out of scope:** downstream DBT models, dashboard logic, alerting, RBAC.

---

## 2. Functional Requirements  

| ID  | Requirement                                                                                       | Notes                                            |
|-----|---------------------------------------------------------------------------------------------------|--------------------------------------------------|
| F1  | **File discovery** – On each run, glob the Dropbox folder for `*.csv`                             | Only process new filenames (use state table)     |
| F2  | **State tracking** – Maintain a `processed_files` table (filename, load_date)                     | Prevent re-processing unless full-reload flag    |
| F3  | **Full vs. incremental load** –  
  - Default: skip files in `processed_files`  
  - Optional CLI flag `--full` to ignore state                                                     |                                                  |
| F4  | **Staging tables** – Auto-create raw staging tables in Postgres for each source                   | DLT’s `CSV` extractor with robust quoting config |
| F5  | **Incremental logic** –  
  - For `items`: snapshot entire file into `item_snapshots` with a `load_date` column  
  - For others: append new rows to staging                                                         |                                                  |
| F6  | **Name matching** – Invoke the existing Python matcher as a DLT post-extract hook on `customers`  | Update or enrich customer table in place         |
| F7  | **Idempotency** – Re-running without `--full` should do nothing for already-processed files       |                                                  |
| F8  | **Error handling** – On row-level errors, log and skip; do not abort the whole run                |                                                  |

---

## 3. Non-Functional Requirements  

- **Deployment:** Docker image containing only Python 3.x, DLT, and dependencies  
- **Configuration:** Single source of truth for secrets via environment variables (e.g. `DATABASE_URL`)  
- **Orchestration:** Cron-driven (`docker run … dlt run pipeline`) or DLT’s scheduler—choose whichever  
- **Logging:** Console output only (DLT logger)  
- **Performance:** Full run < 1 min (current DBT < 1 min)  
- **Environments:** Dev (laptop) vs Prod (VPS) distinguished purely by `DATABASE_URL`  
- **Version control:** Plain `requirements.txt` + `pipeline.py` in Git repo  

---

## 4. Configuration & Secrets  

- **Environment variables** (in your Docker container or `.env`):  
  - `DATABASE_URL` (Postgres connection URI)  
  - `DROPBOX_PATH` (local path where CSVs sync)  
- **CLI flags:**  
  - `--full` to ignore processed state  

---

## 5. Deployment & Run  

1. **Build Docker Image**  
   ```bash
   docker build -t qb-dlt-pipeline .

	2.	Run (daily)

# Cron example: run at 2 AM daily
0 2 * * * docker run --env-file /path/to/.env qb-dlt-pipeline \
  dlt run pipeline


	3.	Full reload

docker run --env-file /path/to/.env qb-dlt-pipeline \
  dlt run pipeline --full



⸻

6. Code Structure

.
├── Dockerfile
├── requirements.txt
├── pipeline.py       # DLT project: extractors, loaders, hooks
└── matcher.py        # Existing Python name-normalizer



⸻

7. Future Extensibility
	•	New sources: Hand-wire additional extractors in pipeline.py
	•	Enrichment: Add Python hooks or DLT transform functions
	•	Alternate implementations: All logic is in one script—can migrate to custom Python if needed

⸻

8. Success Criteria
	•	Zero Meltano; single orchestrated pipeline
	•	One-place configuration (DATABASE_URL)
	•	Daily cron run with < 1 min runtime
	•	New CSVs auto-discovered and idempotently ingested
	•	Name matching applied automatically


