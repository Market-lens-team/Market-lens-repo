import logging
import os
from datetime import datetime, timezone

from google.api_core import retry
from google.cloud import bigquery

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
)

logging.basicConfig(level=logging.INFO)

bq_client = bigquery.Client(project=PROJECT_ID)

SILVER_QUARANTINE_TABLE = "silver_quarantine"

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")


def ensure_silver_tables_exist():
    """
    Creates the Silver dataset (if missing) and both Silver tables
    (if missing), loading each table's schema from its JSON file in
    the schema/ folder. Uses exists_ok=True so this is safe to call
    on every run without erroring on already-existing resources.
    """

    dataset_id = f"{PROJECT_ID}.{SILVER_DATASET}"

    dataset_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=60)

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    dataset_retry(bq_client.create_dataset)(dataset, exists_ok=True)

    table_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=60)

    market_data_schema = bq_client.schema_from_json(
        os.path.join(SCHEMA_DIR, "silver_market_data_schema.json")
    )
    market_data_table = bigquery.Table(
        f"{dataset_id}.{SILVER_TABLE}",
        schema=market_data_schema,
    )
    table_retry(bq_client.create_table)(market_data_table, exists_ok=True)

    metadata_schema = bq_client.schema_from_json(
        os.path.join(SCHEMA_DIR, "silver_symbol_metadata_schema.json")
    )
    metadata_table = bigquery.Table(
        f"{dataset_id}.{SILVER_METADATA_TABLE}",
        schema=metadata_schema,
    )
    table_retry(bq_client.create_table)(metadata_table, exists_ok=True)

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
      2. Clean bad data: zero prices, High < Low, out-of-range Adj_Close.
      3. Join in symbol metadata (security_name, listing_exchange, market_category).
      4. Compute rolling/derived metrics per symbol using window functions.
    """

    silver_target = f"{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}"
    stock_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{STOCK_TABLE}"
    etf_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{ETF_TABLE}"

    query = f"""
    MERGE `{silver_target}` T
    USING (

        WITH combined AS (

            SELECT Date, symbol, 'stock' AS asset_type,
                   Open, High, Low, Close, Adj_Close, Volume
            FROM `{stock_source}`

            UNION ALL

            SELECT Date, symbol, 'etf' AS asset_type,
                   Open, High, Low, Close, Adj_Close, Volume
            FROM `{etf_source}`
        ),

        cleaned AS (

            SELECT
                Date,
                symbol,
                asset_type,

                CASE WHEN Open = 0 THEN NULL ELSE Open END AS Open,
                CASE WHEN High < Low OR High = 0 THEN NULL ELSE High END AS High,
                CASE WHEN High < Low OR Low = 0 THEN NULL ELSE Low END AS Low,
                CASE WHEN Close = 0 THEN NULL ELSE Close END AS Close,

                CASE
                    WHEN Adj_Close < 0 OR Adj_Close > 100000 THEN NULL
                    ELSE Adj_Close
                END AS Adj_Close,

                Volume,

                (High < Low OR Open = 0 OR Close = 0
                 OR Adj_Close < 0 OR Adj_Close > 100000) AS had_quality_issue

            FROM combined
        ),

        flagged AS (

            SELECT
                *,
                (Open IS NOT NULL
                 AND High IS NOT NULL
                 AND Low IS NOT NULL
                 AND Close IS NOT NULL
                 AND Adj_Close IS NOT NULL) AS is_valid
            FROM cleaned
        ),

        with_metrics AS (

            SELECT
                f.*,

                SAFE_DIVIDE(
                    Close - LAG(Close) OVER (
                        PARTITION BY symbol ORDER BY Date
                    ),
                    LAG(Close) OVER (
                        PARTITION BY symbol ORDER BY Date
                    )
                ) AS daily_return_pct,

                AVG(Volume) OVER (
                    PARTITION BY symbol ORDER BY Date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_volume_30d,

                STDDEV(Volume) OVER (
                    PARTITION BY symbol ORDER BY Date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS rolling_stddev_volume_30d,

                MAX(Close) OVER (
                    PARTITION BY symbol ORDER BY Date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS rolling_max_close

            FROM flagged f
        )

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
        -- computed over the full (unfiltered) history per symbol, so a
        -- day's return/rolling stats naturally reflect a gap if the
        -- prior day was invalid, rather than silently interpolating.

    ) S
    ON T.Date = S.Date AND T.symbol = S.symbol AND T.asset_type = S.asset_type

    WHEN MATCHED THEN
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
        run_id = '{run_id}',
        silver_loaded_at = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
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

    return query


def refresh_silver_symbol_metadata() -> int:
    """
    Refreshes the Silver symbol metadata dimension table from Bronze.
    Small table (~8,000 rows) - full refresh each run is cheap and
    simple, no batching/MERGE complexity needed like the price data.
    """

    target = f"{PROJECT_ID}.{SILVER_DATASET}.{SILVER_METADATA_TABLE}"
    source = f"{PROJECT_ID}.{BRONZE_DATASET}.{METADATA_TABLE}"

    query = f"""
    CREATE OR REPLACE TABLE `{target}` AS
    SELECT
        Nasdaq_Traded,
        Symbol AS symbol,
        Security_Name AS security_name,
        Listing_Exchange AS listing_exchange,
        Market_Category AS market_category,
        ETF,
        CASE WHEN ETF = 'Y' THEN 'etf' ELSE 'stock' END AS asset_type,
        Round_Lot_Size AS round_lot_size,
        Test_Issue,
        Financial_Status AS financial_status,
        CQS_Symbol,
        NASDAQ_Symbol,
        NextShares
    FROM `{source}`
    WHERE Test_Issue = 'N'
    """

    query_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=120)

    logging.info("Refreshing silver_symbol_metadata")

    job = bq_client.query(query, location=REGION)
    query_retry(job.result)()

    rows = job.num_dml_affected_rows or 0
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
    re-running Silver never re-inserts the same bad row twice - the
    reason/run_id/quarantined_at just get refreshed on match.

    The invalidity condition here mirrors is_valid in
    build_silver_query() exactly (including IS NULL checks), so a
    row is never silently excluded from silver_market_data without
    also showing up here.
    """

    target = f"{PROJECT_ID}.{QUARANTINE_DATASET}.{SILVER_QUARANTINE_TABLE}"
    stock_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{STOCK_TABLE}"
    etf_source = f"{PROJECT_ID}.{BRONZE_DATASET}.{ETF_TABLE}"

    query = f"""
    MERGE `{target}` T
    USING (

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
      UPDATE SET
        Open = S.Open, High = S.High, Low = S.Low, Close = S.Close,
        Adj_Close = S.Adj_Close, Volume = S.Volume,
        reason = S.reason,
        run_id = '{run_id}',
        quarantined_at = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
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
    """

    query = build_silver_query(run_id)

    query_retry = retry.Retry(
        initial=2.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=480,   # leave headroom under the 540s Cloud Run timeout
    )

    logging.info("Starting Silver transform for run_id=%s", run_id)

    job = bq_client.query(query, location=REGION)
    query_retry(job.result)()

    rows_affected = job.num_dml_affected_rows or 0

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
    """

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{AUDIT_TABLE}"

    record = {
        "batch_id": batch_id,
        "load_type": "SILVER",
        "asset_type": asset_type,
        "gcs_path": None,
        "expected_file_count": None,
        "processed_file_count": None,
        "expected_row_count": None,
        "loaded_row_count": loaded_row_count,
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message[:5000] if error_message else None,
    }

    insert_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=120)
    insert_rows = insert_retry(bq_client.insert_rows_json)

    errors = insert_rows(table_id, [record])

    if errors:
        logging.error("Silver audit insert failed: %s", errors)
    else:
        logging.info("Silver audit record written: %s", batch_id)