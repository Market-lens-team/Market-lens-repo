"""
utils.py

Helper functions used by main.py:
    - ensure_silver_tables_exist : create Silver dataset/tables/quarantine table if missing
    - build_silver_query          : build the Bronze -> Silver MERGE query
    - refresh_silver_symbol_metadata : full refresh of the small metadata dim table
    - write_quarantine_rows       : capture invalid rows with their reason
    - run_silver_transform        : execute the Bronze -> Silver MERGE
    - write_silver_audit          : log a Silver step to ingestion_audit
    - write_gold_ready_marker     : fire the trigger for the Gold Cloud Function

**CHANGED (incremental load fix):**
build_silver_query() previously read the ENTIRE Bronze table on every
run and recomputed rolling window functions over each symbol's full
history, every single time. It now reads only a trailing lookback
window per symbol. See the comments inside build_silver_query() for
the full explanation.
"""

import logging
import os
from datetime import datetime, timezone

from google.api_core import retry       # gives BigQuery calls automatic retry-with-backoff on transient errors
from google.cloud import bigquery       # the BigQuery Python client library

from config import (
    PROJECT_ID,
    REGION,
    BRONZE_DATASET,
    SILVER_DATASET,
    STOCK_TABLE,
    ETF_TABLE,
    METADATA_TABLE,
    SILVER_TABLE,
    SILVER_METADATA_TABLE,
    AUDIT_TABLE,
    QUARANTINE_DATASET,
    SILVER_LOOKBACK_DAYS,  # **CHANGED:** new incremental-window config
)

logging.basicConfig(level=logging.INFO)  # ensures logging.info/warning/exception calls actually print in Cloud Functions logs

bq_client = bigquery.Client(project=PROJECT_ID)  # single shared BigQuery client, reused by every function in this file

SILVER_QUARANTINE_TABLE = "silver_quarantine"  # table name (not full path) for the quarantine table

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")  # folder next to this file holding the 3 JSON schema files


def ensure_silver_tables_exist():
    """
    Creates the Silver dataset (if missing) and both Silver tables
    (if missing), loading each table's schema from its JSON file in
    the schema/ folder. Uses exists_ok=True so this is safe to call
    on every run without erroring on already-existing resources.
    """

    dataset_id = f"{PROJECT_ID}.{SILVER_DATASET}"  # fully-qualified dataset id, e.g. market-lens-506611.silver

    # retry wrapper: if dataset creation transiently fails (network blip etc.), retry with exponential backoff
    dataset_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=60)

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION                                  # must match BQ region used elsewhere or cross-region errors occur
    dataset_retry(bq_client.create_dataset)(dataset, exists_ok=True)  # exists_ok=True = no-op if dataset already exists

    # separate retry wrapper reused for all three table-creation calls below
    table_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=60)

    # load silver_market_data's schema from its JSON definition file
    market_data_schema = bq_client.schema_from_json(
        os.path.join(SCHEMA_DIR, "silver_market_data_schema.json")
    )
    market_data_table = bigquery.Table(
        f"{dataset_id}.{SILVER_TABLE}",
        schema=market_data_schema,
    )
    table_retry(bq_client.create_table)(market_data_table, exists_ok=True)  # create only if it doesn't already exist

    # load silver_symbol_metadata's schema
    metadata_schema = bq_client.schema_from_json(
        os.path.join(SCHEMA_DIR, "silver_symbol_metadata_schema.json")
    )
    metadata_table = bigquery.Table(
        f"{dataset_id}.{SILVER_METADATA_TABLE}",
        schema=metadata_schema,
    )
    table_retry(bq_client.create_table)(metadata_table, exists_ok=True)

    # load silver_quarantine's schema - note this table lives in a DIFFERENT dataset (QUARANTINE_DATASET, not SILVER_DATASET)
    quarantine_schema = bq_client.schema_from_json(
        os.path.join(SCHEMA_DIR, "silver_quarantine_schema.json")
    )
    quarantine_table = bigquery.Table(
        f"{PROJECT_ID}.{QUARANTINE_DATASET}.{SILVER_QUARANTINE_TABLE}",
        schema=quarantine_schema,
    )
    table_retry(bq_client.create_table)(quarantine_table, exists_ok=True)

    logging.info("Silver dataset and tables confirmed/created")


def build_silver_query(run_id: str) -> str:
    """
    Builds the Bronze -> Silver transformation query.

    Steps:
      1. UNION stocks + ETFs into one set, tagging asset_type.
      2. **CHANGED:** Bound each symbol's Bronze rows to a trailing
         lookback window instead of that symbol's full history, using
         a per-symbol watermark (MAX(Date) already in Silver). A
         symbol with no watermark yet (never loaded into Silver) is
         NOT bounded - it gets its full available history once.
      3. Clean bad data: zero prices, High < Low, out-of-range Adj_Close.
      4. Join in symbol metadata (security_name, listing_exchange, market_category).
      5. Compute rolling/derived metrics per symbol using window functions,
         over the bounded window from step 2.
      6. **CHANGED:** rolling_max_close is an ALL-TIME running max, which
         a trimmed window alone cannot compute correctly. It is now
         seeded from the highest rolling_max_close Silver already has
         on record for that symbol.

    Returns the query text as a Python string (f-string), NOT executed
    here - run_silver_transform() below is what actually runs it.
    """

    # fully-qualified table names, built once so the f-string body below stays readable
    silver_target = f"{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}"
    stock_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{STOCK_TABLE}"
    etf_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{ETF_TABLE}"

    query = f"""
    -- Top-level statement: MERGE new/updated Silver rows into silver_market_data.
    -- T = target (existing Silver table), S = source (freshly computed rows below).
    MERGE `{silver_target}` T
    USING (

        -- **CHANGED:** per-symbol watermark - the latest Date each
        -- symbol already has recorded in Silver. A symbol that has
        -- never been loaded here has NULL, which the WHERE clause
        -- below treats as "no lower bound, read everything".
        WITH watermarks AS (

            SELECT symbol, MAX(Date) AS last_silver_date
            FROM `{silver_target}`
            GROUP BY symbol
        ),

        -- **CHANGED:** the highest rolling_max_close Silver already
        -- has on record per symbol. Used later to seed the running
        -- max so an all-time high set outside the lookback window
        -- isn't lost.
        prior_max AS (

            SELECT symbol, MAX(rolling_max_close) AS prior_rolling_max_close
            FROM `{silver_target}`
            GROUP BY symbol
        ),

        -- combined = every raw stock+ETF row this run will consider,
        -- BEFORE cleaning/validation/window functions are applied.
        combined AS (

            -- **CHANGED:** stock rows bounded to the trailing lookback
            -- window per symbol (or unbounded for a brand-new symbol).
            SELECT b.Date, b.symbol, 'stock' AS asset_type,
                   b.Open, b.High, b.Low, b.Close, b.Adj_Close, b.Volume
            FROM `{stock_source}` b
            LEFT JOIN watermarks w ON w.symbol = b.symbol   -- attach each row's symbol watermark (NULL if new symbol)
            WHERE w.last_silver_date IS NULL                 -- new symbol: no lower bound, take everything
               OR b.Date >= DATE_SUB(w.last_silver_date, INTERVAL {SILVER_LOOKBACK_DAYS} DAY)  -- existing symbol: only the trailing window

            UNION ALL   -- combine stocks + ETFs into one row set; ALL keeps duplicates (there shouldn't be any across the two source tables)

            -- **CHANGED:** identical lookback-bounding logic, applied to ETF rows
            SELECT b.Date, b.symbol, 'etf' AS asset_type,
                   b.Open, b.High, b.Low, b.Close, b.Adj_Close, b.Volume
            FROM `{etf_source}` b
            LEFT JOIN watermarks w ON w.symbol = b.symbol
            WHERE w.last_silver_date IS NULL
               OR b.Date >= DATE_SUB(w.last_silver_date, INTERVAL {SILVER_LOOKBACK_DAYS} DAY)
        ),

        -- cleaned = same rows as combined, but bad values are NULLed out
        -- (not dropped - dropping happens later via the is_valid filter)
        cleaned AS (

            SELECT
                Date,
                symbol,
                asset_type,

                CASE WHEN Open = 0 THEN NULL ELSE Open END AS Open,                     -- zero Open is treated as missing data
                CASE WHEN High < Low OR High = 0 THEN NULL ELSE High END AS High,       -- High below Low, or zero, is invalid
                CASE WHEN High < Low OR Low = 0 THEN NULL ELSE Low END AS Low,          -- same check mirrored for Low
                CASE WHEN Close = 0 THEN NULL ELSE Close END AS Close,                  -- zero Close is treated as missing data

                CASE
                    WHEN Adj_Close < 0 OR Adj_Close > 100000 THEN NULL                  -- sanity bounds on adjusted close price
                    ELSE Adj_Close
                END AS Adj_Close,

                Volume,   -- Volume is passed through unchanged (no cleaning rule applied to it here)

                -- boolean flag: TRUE if ANY of the above conditions triggered on this row
                (High < Low OR Open = 0 OR Close = 0
                 OR Adj_Close < 0 OR Adj_Close > 100000) AS had_quality_issue

            FROM combined
        ),

        -- flagged = cleaned rows, plus a single is_valid flag summarizing
        -- whether every required field survived cleaning (non-NULL)
        flagged AS (

            SELECT
                *,
                (Open IS NOT NULL
                 AND High IS NOT NULL
                 AND Low IS NOT NULL
                 AND Close IS NOT NULL
                 AND Adj_Close IS NOT NULL) AS is_valid   -- a row needs ALL five price fields present to count as valid
            FROM cleaned
        ),

        -- with_metrics = flagged rows, plus every derived/rolling metric computed via window functions
        with_metrics AS (

            SELECT
                f.*,

                -- % change vs the previous trading day's Close, per symbol, in date order
                SAFE_DIVIDE(
                    Close - LAG(Close) OVER (
                        PARTITION BY f.symbol ORDER BY f.Date
                    ),
                    LAG(Close) OVER (
                        PARTITION BY f.symbol ORDER BY f.Date
                    )
                ) AS daily_return_pct,   -- SAFE_DIVIDE returns NULL instead of erroring on divide-by-zero (e.g. first row per symbol)

                -- rolling 30-trading-day average volume (current row + 29 preceding), per symbol
                AVG(Volume) OVER (
                    PARTITION BY f.symbol ORDER BY f.Date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_volume_30d,

                -- rolling 30-trading-day standard deviation of volume - used later in Gold for volume_zscore
                STDDEV(Volume) OVER (
                    PARTITION BY f.symbol ORDER BY f.Date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS rolling_stddev_volume_30d,

                -- **CHANGED:** all-time running max Close price. GREATEST()
                -- compares two things: (a) the max Close seen WITHIN this
                -- run's bounded window, and (b) whatever max Silver already
                -- had on record for this symbol (prior_max, seeded via the
                -- LEFT JOIN below) - so a historical all-time-high outside
                -- the lookback window is never lost.
                GREATEST(
                    MAX(Close) OVER (
                        PARTITION BY f.symbol ORDER BY f.Date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- unbounded = from the first row in THIS window through the current row
                    ),
                    COALESCE(pm.prior_rolling_max_close, 0)   -- COALESCE handles a brand-new symbol with no prior_max row yet
                ) AS rolling_max_close

            FROM flagged f
            LEFT JOIN prior_max pm ON pm.symbol = f.symbol   -- **CHANGED:** attach each symbol's previously-recorded max
        )

        -- Final SELECT: only VALID rows get MERGEd into Silver.
        -- Invalid rows are intentionally excluded here - they go to
        -- silver_quarantine instead, via write_quarantine_rows() below.
        SELECT
            w.Date,
            w.symbol,
            w.asset_type,
            w.Open,
            w.High,
            w.Low,
            w.Close,
            w.Adj_Close,
            w.Volume,
            w.is_valid,
            w.daily_return_pct,
            w.rolling_avg_volume_30d,
            w.rolling_stddev_volume_30d,
            w.rolling_max_close

        FROM with_metrics w
        WHERE w.is_valid = TRUE
        -- Invalid rows are excluded here; they are captured separately
        -- in silver_quarantine via write_quarantine_rows(), with their
        -- original values and a reason. Rolling metrics above are still
        -- computed over the bounded (not full) history per symbol, so a
        -- day's return/rolling stats naturally reflect a gap if the
        -- prior day was invalid, rather than silently interpolating.

    ) S
    -- MERGE match key: a row is "the same" if Date, symbol, AND asset_type all match
    ON T.Date = S.Date AND T.symbol = S.symbol AND T.asset_type = S.asset_type

    WHEN MATCHED THEN
      -- row already exists in Silver -> overwrite every computed column with the freshly recomputed value
      UPDATE SET
        Open = S.Open,
        High = S.High,
        Low = S.Low,
        Close = S.Close,
        Adj_Close = S.Adj_Close,
        Volume = S.Volume,
        is_valid = S.is_valid,
        daily_return_pct = S.daily_return_pct,
        rolling_avg_volume_30d = S.rolling_avg_volume_30d,
        rolling_stddev_volume_30d = S.rolling_stddev_volume_30d,
        rolling_max_close = S.rolling_max_close,
        run_id = '{run_id}',                     -- stamps which run last touched this row, for audit correlation
        silver_loaded_at = CURRENT_TIMESTAMP()   -- stamps when this row was last (re)written

    WHEN NOT MATCHED THEN
      -- row is new to Silver -> insert it
      INSERT (
        Date, symbol, asset_type, Open, High, Low, Close, Adj_Close, Volume,
        is_valid, daily_return_pct, rolling_avg_volume_30d,
        rolling_stddev_volume_30d, rolling_max_close,
        run_id, silver_loaded_at
      )
      VALUES (
        S.Date, S.symbol, S.asset_type, S.Open, S.High, S.Low, S.Close,
        S.Adj_Close, S.Volume, S.is_valid, S.daily_return_pct,
        S.rolling_avg_volume_30d, S.rolling_stddev_volume_30d,
        S.rolling_max_close, '{run_id}', CURRENT_TIMESTAMP()
      )
    """

    return query   # caller (run_silver_transform) is responsible for actually executing this


def refresh_silver_symbol_metadata() -> int:
    """
    Refreshes the Silver symbol metadata dimension table from Bronze.
    Small table (~8,000 rows) - full refresh each run is cheap and
    simple, no batching/MERGE complexity needed like the price data.
    NOT changed for the incremental fix - this table is small enough
    that a full refresh every run is not the cost problem.
    """

    target = f"{PROJECT_ID}.{SILVER_DATASET}.{SILVER_METADATA_TABLE}"
    source = f"{PROJECT_ID}.{BRONZE_DATASET}.{METADATA_TABLE}"

    # CREATE OR REPLACE TABLE = full wipe-and-rebuild every run (intentional here, unlike the price data)
    query = f"""
    CREATE OR REPLACE TABLE `{target}` AS
    SELECT
        Nasdaq_Traded,
        Symbol AS symbol,                                          -- renamed to lowercase to match Silver's naming convention
        Security_Name AS security_name,
        Listing_Exchange AS listing_exchange,
        Market_Category AS market_category,
        ETF,
        CASE WHEN ETF = 'Y' THEN 'etf' ELSE 'stock' END AS asset_type,  -- derives asset_type from the Y/N ETF flag
        Round_Lot_Size AS round_lot_size,
        Test_Issue,
        Financial_Status AS financial_status,
        CQS_Symbol,
        NASDAQ_Symbol,
        NextShares
    FROM `{source}`
    WHERE Test_Issue = 'N'    -- excludes test/placeholder ticker symbols from the real dataset
    """

    query_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=120)

    logging.info("Refreshing silver_symbol_metadata")

    job = bq_client.query(query, location=REGION)
    query_retry(job.result)()   # .result() blocks until the query finishes; wrapped in retry for transient failures

    rows = job.num_dml_affected_rows or 0   # `or 0` guards against None if the API doesn't report a count
    logging.info("silver_symbol_metadata refreshed, rows=%s", rows)

    return rows


def write_quarantine_rows(run_id: str) -> int:
    """
    Captures rows that failed validation, with their ORIGINAL
    (uncleaned) values, into silver_quarantine for inspection.
    Runs against the same Bronze sources as the main transform, so
    the reason for rejection is visible (e.g. Open=0, Adj_Close
    out of range, or NULL in the source) rather than the NULLed
    version.

    Uses MERGE (keyed on Date + symbol + asset_type), not INSERT, so
    re-running Silver never re-inserts the same bad row twice.

    NOT changed for the incremental fix - intentionally left as a
    full scan (see reasoning in the docstring further below).
    """

    target = f"{PROJECT_ID}.{QUARANTINE_DATASET}.{SILVER_QUARANTINE_TABLE}"
    stock_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{STOCK_TABLE}"
    etf_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{ETF_TABLE}"

    query = f"""
    MERGE `{target}` T
    USING (

        -- combined = every raw stock+ETF row (NOT bounded by lookback -
        -- deliberately reads full history so a bad row is never
        -- permanently missed just because it fell outside a window)
        WITH combined AS (
            SELECT Date, symbol, 'stock' AS asset_type,
                   Open, High, Low, Close, Adj_Close, Volume
            FROM `{stock_source}`

            UNION ALL

            SELECT Date, symbol, 'etf' AS asset_type,
                   Open, High, Low, Close, Adj_Close, Volume
            FROM `{etf_source}`
        )

        SELECT
            Date, symbol, asset_type, Open, High, Low, Close, Adj_Close, Volume,

            -- builds a human-readable reason string by testing each
            -- invalidity condition individually and joining whichever
            -- ones are true; NULLs from the non-matching IFs are
            -- dropped by ARRAY_TO_STRING automatically
            ARRAY_TO_STRING([
                IF(Open IS NULL, 'null_open', NULL),
                IF(Open = 0, 'zero_open', NULL),
                IF(High IS NULL, 'null_high', NULL),
                IF(High = 0, 'zero_high', NULL),
                IF(Low IS NULL, 'null_low', NULL),
                IF(Low = 0, 'zero_low', NULL),
                IF(Close IS NULL, 'null_close', NULL),
                IF(Close = 0, 'zero_close', NULL),
                IF(Adj_Close IS NULL, 'null_adj_close', NULL),
                IF(High < Low, 'high_less_than_low', NULL),
                IF(Adj_Close < 0 OR Adj_Close > 100000, 'adj_close_out_of_range', NULL)
            ], ', ') AS reason

        FROM combined
        -- mirrors is_valid's logic in build_silver_query() exactly, so
        -- every row excluded there is guaranteed to appear here too
        WHERE Open IS NULL OR Open = 0
           OR High IS NULL OR High = 0
           OR Low IS NULL OR Low = 0
           OR Close IS NULL OR Close = 0
           OR Adj_Close IS NULL
           OR High < Low
           OR Adj_Close < 0 OR Adj_Close > 100000

    ) S
    ON T.Date = S.Date AND T.symbol = S.symbol AND T.asset_type = S.asset_type

    WHEN MATCHED THEN
      -- same bad row seen again -> refresh its recorded values/reason/timestamp rather than duplicate it
      UPDATE SET
        Open = S.Open, High = S.High, Low = S.Low, Close = S.Close,
        Adj_Close = S.Adj_Close, Volume = S.Volume,
        reason = S.reason,
        run_id = '{run_id}',
        quarantined_at = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
      -- newly discovered bad row -> insert it
      INSERT (Date, symbol, asset_type, Open, High, Low, Close, Adj_Close,
              Volume, reason, run_id, quarantined_at)
      VALUES (S.Date, S.symbol, S.asset_type, S.Open, S.High, S.Low, S.Close,
              S.Adj_Close, S.Volume, S.reason, '{run_id}', CURRENT_TIMESTAMP())
    """

    query_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=120)

    logging.info("Writing quarantined rows for run_id=%s", run_id)

    job = bq_client.query(query, location=REGION)
    query_retry(job.result)()

    rows = job.num_dml_affected_rows or 0
    logging.info("Quarantine write complete, rows=%s", rows)

    return rows


def run_silver_transform(run_id: str) -> int:
    """
    Executes the Bronze -> Silver MERGE and returns rows affected.
    Wrapped in retry to absorb transient BigQuery/connection errors.
    NOT changed - still just calls build_silver_query(); the
    incremental-window logic lives inside that function's SQL.
    """

    query = build_silver_query(run_id)   # builds the full MERGE statement as a string, doesn't execute it yet

    # deadline is deliberately set below Cloud Run's 540s hard timeout,
    # leaving headroom so this function fails gracefully with a caught
    # exception rather than being killed mid-query
    query_retry = retry.Retry(
        initial=2.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=480,   # leave headroom under the 540s Cloud Run timeout
    )

    logging.info("Starting Silver transform for run_id=%s", run_id)

    job = bq_client.query(query, location=REGION)   # submits the query as an async BigQuery job
    query_retry(job.result)()                        # blocks until the job completes (or times out)

    rows_affected = job.num_dml_affected_rows or 0   # total rows touched by the MERGE (both matched + inserted)

    logging.info(
        "Silver transform complete for run_id=%s, rows affected=%s",
        run_id, rows_affected,
    )

    return rows_affected


def write_silver_audit(
    batch_id: str,
    asset_type: str,
    status: str,
    started_at,
    loaded_row_count: int = 0,
    error_message: str = None,
):
    """
    Logs a Silver run to the shared ingestion_audit table, matching
    Bronze's real schema exactly. File-specific columns (gcs_path,
    file counts) don't apply to Silver's transform, so they're left
    NULL. status is uppercase ('SUCCESS'/'FAILED') to match Bronze.
    NOT changed.
    """

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{AUDIT_TABLE}"   # note: physically lives in bronze dataset, shared by all layers

    # builds the exact row dict that insert_rows_json expects - keys must match the BigQuery table's column names
    record = {
        "batch_id": batch_id,
        "load_type": "SILVER",                 # distinguishes this row from BRONZE/GOLD rows in the same shared table
        "asset_type": asset_type,               # e.g. "silver_market_data", "silver_symbol_metadata", "silver_quarantine"
        "gcs_path": None,                       # not applicable to Silver (no source file path, unlike Bronze)
        "expected_file_count": None,
        "processed_file_count": None,
        "expected_row_count": None,
        "loaded_row_count": loaded_row_count,
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message[:5000] if error_message else None,  # truncated to 5000 chars to avoid oversized rows
    }

    insert_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=120)
    insert_rows = insert_retry(bq_client.insert_rows_json)   # streaming insert - fast, but eventually-consistent for reads

    errors = insert_rows(table_id, [record])   # takes a LIST of rows even though we're only inserting one

    if errors:
        # insert_rows_json doesn't raise on failure - it returns a list of per-row errors instead, so this MUST be checked
        logging.error("Silver audit insert failed: %s", errors)
    else:
        logging.info("Silver audit record written: %s", batch_id)


from google.cloud import storage   # imported here (mid-file) rather than at the top - matches the original file's structure

GOLD_TRIGGER_BUCKET = "market-lens-506611-raw-mlteam-2026"   # same bucket as everything else, named separately for clarity here
GOLD_TRIGGER_PREFIX = "gold_trigger"                          # (defined but not directly used below - marker path is built inline)

storage_client = storage.Client()   # NOTE: unlike bq_client above, this doesn't pass project= explicitly - relies on ADC's default project


def write_gold_ready_marker(run_id: str):
    """
    Writes an empty marker file to GCS once Silver has fully
    completed (market data MERGE + metadata refresh + quarantine
    write all succeeded). This upload fires the GCS trigger on the
    silver-to-gold Cloud Function, which watches for files matching
    gold_trigger/<run_id>/_ready and ignores every other file event.

    Path format matches Gold's main.py check (len(parts) == 3):
        gold_trigger/<run_id>/_ready

    Call this only after Silver's quarantine step succeeds - if
    anything upstream raises, this line is never reached, so Gold
    never fires on incomplete/bad Silver data.
    NOT changed.
    """

    bucket = storage_client.bucket(GOLD_TRIGGER_BUCKET)

    marker_path = f"gold_trigger/{run_id}/_ready"   # this exact 3-part path shape is what Gold's main.py parses

    blob = bucket.blob(marker_path)

    blob.upload_from_string(
        "",                       # empty file - only its EXISTENCE/PATH matters, not its content
        content_type="text/plain",
    )

    logging.info(
        "Gold trigger marker written: %s",
        marker_path,
    )