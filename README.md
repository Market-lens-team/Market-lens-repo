# Market-lens-repo

# MarketLens

**A governed data pipeline and AI analyst for market-data questions**

Built as a data engineering project — turning messy, per-ticker stock and ETF CSV files into a clean, queryable, dashboard- and AI-agent-ready dataset on Google Cloud.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Objective](#2-objective)
3. [About the Data](#3-about-the-data)
4. [Architecture Overview](#4-architecture-overview)
5. [Infrastructure as Code](#5-infrastructure-as-code)
6. [The Three Layers](#6-the-three-layers)
7. [Audit and Traceability](#7-audit-and-traceability)
8. [Challenges Identified and Resolved](#8-challenges-identified-and-resolved)
9. [Known Limitations](#9-known-limitations)
10. [Dashboard](#10-dashboard)
11. [AI Analyst (Semantic Layer)](#11-ai-analyst-semantic-layer)
12. [Deployment Guide](#12-deployment-guide)
13. [Tech Stack](#13-tech-stack)
14. [What's Next](#14-whats-next)

---

## 1. Problem Statement

People want to ask simple questions about the stock market — like *"which mid-cap stocks had unusual volume spikes last quarter?"* or *"compare AAPL against its sector peers over the last year."* The raw data needed to answer these questions exists, but only as hundreds of separate, messy per-ticker CSV files, with no cleaning, no structure, and no way to query across them.

MarketLens turns that raw, scattered data into something queryable, governed, and safe to build a product on top of.

---

## 2. Objective

Build a working MVP data pipeline that:

1. Reliably loads raw per-ticker CSV files (stocks and ETFs) into a database
2. Cleans and validates the data, with a recorded reason for anything rejected
3. Computes the metrics people actually want to ask about — rolling averages, period returns, volume anomalies, drawdown
4. Organizes the result into simple, well-named tables that are easy to query directly
5. Runs automatically off new data landing in storage — no manual steps
6. Processes new data efficiently, without reprocessing full history on every run
7. Surfaces the result through both a dashboard and a natural-language AI agent

---

## 3. About the Data

**Raw files**: one CSV per ticker (e.g. `AAPL.csv`), where the ticker symbol is only known from the filename, not a column inside the file. Each file has daily `Open`, `High`, `Low`, `Close`, `Adj_Close`, and `Volume`. A separate metadata file (`symbols_valid_meta.csv`) lists every ticker's name, exchange, and listing category.

**Two load types**:
- **Historical** — a large, occasional load of a ticker's full price history
- **Incremental** — smaller, ongoing loads of just the newest day(s)

**GCS bucket layout**:
```
historical/
├── stocks/*.csv
├── etfs/*.csv
├── symbols_valid_meta.csv
└── _READY                <- uploaded LAST, signals "ready to process"

incremental/
├── stocks/*.csv
├── etfs/*.csv
├── symbols_valid_meta.csv   (optional)
└── _READY
```
`_READY` is an empty file — only its existence and exact path matter. Nothing is processed until it appears.

---

## 4. Architecture Overview

MarketLens uses a **medallion architecture** on Google Cloud: three progressively cleaner layers (Bronze → Silver → Gold), each triggering the next automatically via a marker file appearing in GCS. There is no central orchestrator — each Cloud Function reacts to a file the previous one writes.

```
GCS bucket (raw CSVs + _READY)
        │
        ▼
   Bronze — gcs_to_bronze          (loads raw data, MERGE-based)
        │  writes silver_trigger/<run_id>/_ready
        ▼
   Silver — bronze_to_silver       (cleans, validates, computes metrics)
        │  writes gold_trigger/<run_id>/_ready
        ▼
   Gold — silver_to_gold           (builds facts, marts, views)
        │
        ├──► Looker Studio dashboard
        └──► AI Analyst (semantic layer)
```

Every layer writes its own SUCCESS/FAILED row to one **shared audit table** (`bronze.ingestion_audit`), tagged with a common `run_id`, so the entire pipeline is traceable end to end from a single trigger.

---

## 5. Infrastructure as Code

All infrastructure is provisioned via **Terraform**, not manual console setup:

- GCS bucket (raw file storage)
- BigQuery datasets: `bronze`, `silver`, `quarantined`, `gold`
- The Cloud Functions' service account, with scoped IAM (`BigQuery Data Editor`, `BigQuery Job User`, `Storage Object Admin`)
- The three Cloud Functions themselves (as deployment targets)

This makes the environment reproducible and version-controlled rather than living only in a console click-history.

---

## 6. The Three Layers

### 6.1 Bronze — raw ingestion

- Cloud Function: `gcs_to_bronze`
- Loads raw CSVs into BigQuery via **external tables** (a pointer into GCS, no data copied) and **MERGE** (idempotent — safe to re-run without creating duplicates)
- Batched (500 files per batch) for large file counts, with one audit row per batch
- Extracts the ticker symbol from each file's **name** (not a column) via `REGEXP_EXTRACT(_FILE_NAME, ...)`
- De-duplicates rows within a single batch (`QUALIFY ROW_NUMBER()`) so one duplicate row in a source CSV can't fail an entire batch

**Tables**: `bronze_stock_prices`, `bronze_etf_prices`, `bronze_symbol_metadata`, `ingestion_audit` (shared)

### 6.2 Silver — cleaned and enriched

- Cloud Function: `bronze_to_silver`
- Validates every row: rejects zero prices, `High < Low`, and out-of-range `Adj_Close`
- Invalid rows are excluded from `silver_market_data` and routed to `silver_quarantine` with a specific reason
- Valid rows are enriched with `daily_return_pct`, 30-day rolling volume average/stddev, and rolling all-time-high close
- **Incremental**: only re-reads a trailing lookback window (45 days) per symbol, using a per-symbol watermark against Silver's own data — not a full-history rebuild on every run

**Tables**: `silver_market_data`, `silver_symbol_metadata`, `silver_quarantine` (separate `quarantined` dataset)

### 6.3 Gold — built for questions

- Cloud Function: `silver_to_gold`, calling stored procedure `gold.sp_refresh_all(run_id)`
- **Incremental**: `fact_daily_metrics` uses the same watermark approach as Silver (300-day lookback, since its longest rolling window is 200 days)
- Deprecated objects are actively dropped on every run (step 0.5 of the procedure), so nothing unused lingers

**Current Gold objects** (10 total):

| Type | Objects |
|---|---|
| Dimension | `dim_security` |
| Facts | `fact_daily_metrics`, `fact_period_returns` |
| Marts | `mart_screener`, `mart_normalized_prices`, `mart_unusual_volume`, `mart_sector_summary` |
| Views | `v_top_returns`, `v_normalized_price_comparison`, `v_sector_comparison` |

Every object here traces directly to one of the two problem-statement questions, or to general screening. Objects that didn't (`dim_date`, `fact_drawdown_yearly`, and several unused views) were deliberately removed.

---

## 7. Audit and Traceability

Every processing step, across all three layers, writes a SUCCESS or FAILED row to the shared `bronze.ingestion_audit` table, tagged with a common `run_id`. This makes any run fully traceable — which batch failed, when, and why — without guessing.

```sql
SELECT batch_id, load_type, asset_type, loaded_row_count, status, started_at
FROM `market-lens-506611.bronze.ingestion_audit`
ORDER BY started_at DESC
LIMIT 20;
```

---

## 8. Challenges Identified and Resolved

### 8.1 Pipeline wasn't truly incremental
Silver and Gold originally reprocessed a symbol's **entire** history on every single run, even for one new day. Fixed with a per-symbol watermark and a bounded trailing lookback window (45 days for Silver, 300 for Gold — sized to each layer's longest rolling calculation).

### 8.2 A silent sorting bug in Gold's deduplication
`gold_base`'s duplicate-resolution step originally sorted by `run_id` (a text string like `historical_...`/`incremental_...`) instead of an actual timestamp. Since `"i"` sorts after `"h"` alphabetically, an old incremental run could silently outrank a brand-new historical correction. Fixed by sorting on `silver_loaded_at` (a real `TIMESTAMP`) instead.

### 8.3 One bad row could fail an entire Bronze batch
A duplicate row for the same ticker and date within a single source CSV would previously crash the whole load batch outright (BigQuery's MERGE requirement: at most one source row per target match). Fixed with `QUALIFY ROW_NUMBER()` deduplication inside the load query itself, before the MERGE join.

### 8.4 A corrupted source price produced fake extreme returns
Manual investigation surfaced tickers where a price was recorded as an absurd value for one or two days (e.g. a $43,000 stock briefly showing $0.17) before recovering — almost certainly a data entry error, not a real market event. This produced calculated returns of thousands of percent. Silver's validation only rejects a price of exactly `0`, so this slipped through. **Fix**: a plausibility filter added in `mart_screener` and `mart_normalized_prices` — excludes any return outside roughly `-99%` to `+2000%`, or any indexed price outside `1` to `100,000` — protecting the dashboard-facing tables without reprocessing full Silver history.

### 8.5 Quarantine's `run_id` was being overwritten on every run
`silver_quarantine`'s MERGE previously updated `run_id` every time a still-invalid row was re-scanned, meaning a row first caught during the historical load would show `run_id = 'incremental_...'` after any later incremental run touched it — losing the answer to "which load originally caught this." **Fix**: `run_id` is no longer touched on `WHEN MATCHED`, only set once at first insert; `quarantined_at` still refreshes on every re-confirmation.

---

## 9. Known Limitations

**No industry/sector data.** `symbols_valid_meta.csv` has no industry/sector field — `market_category` is a Nasdaq **listing tier** code (`Q`/`G`/`S`), not an industry classification. A correlation-based statistical alternative (grouping symbols by how closely their price movements correlate) was built and tested, but required more trading history than the dataset currently has to produce reliable results, and was removed. `mart_sector_summary`/`v_sector_comparison` are kept as an honestly-labeled fallback — real data, clearly captioned as "listing tier" everywhere it's shown, never as "sector."

**No yearly drawdown breakdown table.** `fact_drawdown_yearly` was removed as it didn't trace to either problem-statement question. The underlying daily `drawdown_pct` values are still available in `fact_daily_metrics` for every historical day, so year-specific drawdown questions remain answerable via ad-hoc aggregation (see Section 11) — just not pre-computed into a standing table.

**Silver's validation catches zero/impossible prices, not implausible ones.** A price like `$0.17` on a stock that normally trades at $43,000+ is non-zero and technically passes validation, even though it's clearly wrong. This is mitigated downstream (Section 8.4) rather than fixed at the source, given project timeline constraints.

---

## 10. Dashboard

Built in **Looker Studio**, reading directly from the Gold views/marts above.

**Panels**:
- KPI row — total securities, positive 1-year performers, stock/ETF counts
- Symbol vs. listing tier average (`v_sector_comparison`) — with an explicit caption noting this reflects listing tier, not industry sector
- Top performers, split by stock/ETF (`v_top_returns`)
- Unusual volume screener (`mart_unusual_volume`)
- Normalized price comparison (`v_normalized_price_comparison`)
- Multi-period return breakdown, per selected symbol (`mart_screener`)

---

## 11. AI Analyst (Semantic Layer)

A natural-language agent sits on top of the Gold layer, translating plain-English questions into BigQuery queries via a defined semantic layer (system prompt).

**Example questions it can answer**:
- *"Show me the top 10 stocks by 1-year return"* → `v_top_returns`
- *"Was TSLA's volume unusual last month?"* → ad-hoc query against `fact_daily_metrics`, filtered by date range and `ABS(volume_zscore) > 2`
- *"Compare MSFT, GOOGL, and AAPL over the last 3 years, normalized"* → `mart_normalized_prices`, filtered by symbol and date range
- *"Which ETFs had the lowest drawdown in 2022?"* → ad-hoc aggregation of `fact_daily_metrics` (`MIN(drawdown_pct)` grouped by symbol, filtered to 2022)

**Design principle**: the agent is instructed to write its own aggregation SQL directly against `fact_daily_metrics`/`fact_period_returns` whenever a question doesn't match a pre-built mart exactly (a specific past year, a custom date range) — rather than declaring the data unavailable. The **one** genuine hard limit it enforces is industry/sector filtering, which it explicitly declines and explains, since that data doesn't exist anywhere in the pipeline.

---

## 12. Deployment Guide

### Deploy a Cloud Function
```powershell
gcloud functions deploy <function-name> `
  --gen2 `
  --runtime=python312 `
  --region=us-central1 `
  --source=. `
  --entry-point=<entry_point_function> `
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" `
  --trigger-event-filters="bucket=market-lens-506611-raw-mlteam-2026" `
  --memory=512MB `
  --timeout=540s `
  --service-account=marketlens-cloud-function-sa@market-lens-506611.iam.gserviceaccount.com `
  --project=market-lens-506611
```

| Function | `--entry-point` |
|---|---|
| Bronze | `gcs_to_bronze` |
| Silver | `bronze_to_silver` |
| Gold | `silver_to_gold` |

### Deploy the Gold stored procedure
```powershell
Get-Content -Raw .\sp_gold.sql | bq query --use_legacy_sql=false
```

### Verify a run
```sql
SELECT batch_id, status, loaded_row_count, error_message, started_at
FROM `market-lens-506611.bronze.ingestion_audit`
ORDER BY started_at DESC
LIMIT 20;
```

---

## 13. Tech Stack

| Purpose | Technology |
|---|---|
| File storage | Google Cloud Storage |
| Compute | Cloud Functions (Gen 2, Python 3.12) |
| Data warehouse | BigQuery |
| Infrastructure as code | Terraform |
| Dashboard | Looker Studio |
| AI Analyst | LLM agent + BigQuery-executing semantic layer |

---

## 14. What's Next

- Extend Silver's validation to catch implausible-but-non-zero prices (compare each price against a symbol's own recent range, not just check for zero)
- Bring in a real industry/sector reference dataset if one becomes available, to replace the listing-tier fallback
- Broader monitoring/alerting on `ingestion_audit` FAILED rows
- Revisit the correlation-based peer-grouping approach once more trading history has accumulated

##TO DEPLOY THE CLOUD FUNCTIONS
gcloud functions deploy bronze-to-silver `                                                        
>>   --gen2 `                                                       
>>   --runtime=python312 `                                         
>>   --region=us-central1 `
>>   --source=. `    
>>   --entry-point=bronze_to_silver `                               
>>   --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" `                       
>>   --trigger-event-filters="bucket=market-lens-506611-raw-mlteam-2026" `
>>   --memory=512MB `
>>   --timeout=540s `
>>   --service-account=marketlens-cloud-function-sa@market-lens-506611.iam.gserviceaccount.com `
>>   --project=market-lens-506611


-lens-repo\transform\gold\procedures> gcloud functions deploy load-price-data
 --gen2 
--runtime=python312 
--region=us-central1 
--source=. 
--entry-point=gcs_to_bronze 
--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" 
--trigger-event-filters="bucket=market-lens-506611-raw-mlteam-2026"
 --memory=512MB --timeout=540s 
--service-account=marketlens-cloud-function-sa@marketlens-506611.iam.gserviceaccount.com --project=market-lens-506611




 gcloud functions deploy silver-to-gold --gen2 --runtime=python312 --region=us-central1 --source=. --entry-point=silver_to_gold --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" --trigger-event-filters="bucket=market-lens-506611-raw-mlteam-2026" --memory=512MB --timeout=540s --service-account=marketlens-cloud-function-sa@market-lens-506611.iam.gserviceaccount.com --project=market-lens-506611
