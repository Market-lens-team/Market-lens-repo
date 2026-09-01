"""
utils.py

Helper functions used by main.py:
    - list_csv_files            : list all CSVs under a GCS prefix
    - chunk_list                : split a list into fixed-size batches
    - create_external_table     : point a temporary BQ table at a batch of GCS files
    - get_row_count             : count rows in a table (data-loss check)
    - merge_batch_with_symbol   : MERGE a batch into Bronze (update if exists, insert if not)
    - merge_metadata_file       : MERGE the symbols_valid_meta.csv into bronze_symbol_metadata
    - drop_table                : remove a temporary external table
    - write_audit               : log one row describing how a run went
    - file_exists               : check if a specific file exists in GCS
"""

import logging
from datetime import datetime, timezone

from google.cloud import storage, bigquery

from config import PROJECT_ID, BUCKET_NAME, AUDIT_TABLE


logging.basicConfig(level=logging.INFO)


# ============================================================
# CLIENTS
# ============================================================

storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)


# ============================================================
# GENERAL HELPERS
# ============================================================

def utc_now():
    """Returns the current UTC timestamp."""
    return datetime.now(timezone.utc)


def file_exists(file_path: str) -> bool:
    """Checks whether a specific object exists in the GCS bucket."""

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_path)

    return blob.exists()


def list_csv_files(prefix: str):
    """
    Lists every CSV file under a GCS prefix.

    Returns GCS URIs such as:
        gs://bucket/historical/stocks/AAPL.csv
    """

    blobs = storage_client.list_blobs(
        BUCKET_NAME,
        prefix=prefix
    )

    uris = [
        f"gs://{BUCKET_NAME}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".csv")
    ]

    logging.info(
        "Found %s CSV files under prefix '%s'",
        len(uris),
        prefix
    )

    return uris


def chunk_list(items, size):
    """Splits a list into consecutive chunks of at most 'size' items."""

    for index in range(0, len(items), size):
        yield items[index:index + size]


# ============================================================
# EXTERNAL TABLE FOR PRICE FILES
# ============================================================

def create_external_table(
    uris,
    external_table_id: str,
    schema
):
    """
    Creates a temporary BigQuery external table pointing directly
    at a batch of GCS CSV files.

    Explicit schema is used instead of autodetect because source
    files can contain inconsistent numeric representations.
    """

    # Delete an old external table if one exists.
    # This prevents stale schemas from being reused.
    bq_client.delete_table(
        external_table_id,
        not_found_ok=True
    )

    table = bigquery.Table(external_table_id)

    external_config = bigquery.ExternalConfig("CSV")

    external_config.source_uris = uris

    external_config.options.skip_leading_rows = 1

    external_config.autodetect = False

    external_config.schema = schema

    table.external_data_configuration = external_config

    bq_client.create_table(table)

    logging.info(
        "Created external table %s over %s files",
        external_table_id,
        len(uris)
    )


# ============================================================
# ROW COUNT
# ============================================================

def get_row_count(table_id: str) -> int:
    """
    Runs COUNT(*) against any table, including an external table.
    """

    query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{table_id}`
    """

    result = bq_client.query(query).result()

    for row in result:
        return row.row_count

    return 0


# ============================================================
# PRICE FILE SCHEMA
# ============================================================

PRICE_FILE_SCHEMA = [
    bigquery.SchemaField("Date", "STRING"),
    bigquery.SchemaField("Open", "FLOAT64"),
    bigquery.SchemaField("High", "FLOAT64"),
    bigquery.SchemaField("Low", "FLOAT64"),
    bigquery.SchemaField("Close", "FLOAT64"),
    bigquery.SchemaField("Adj_Close", "FLOAT64"),
    bigquery.SchemaField("Volume", "FLOAT64"),
]


# ============================================================
# MERGE PRICE DATA
# ============================================================

def merge_batch_with_symbol(
    external_table_id: str,
    target_table_id: str
) -> int:
    """
    Merges every row from the external price table into the
    target Bronze table.

    Symbol is extracted from the source CSV filename.

    Matching key:
        symbol + Date

    If the row already exists:
        UPDATE

    If the row does not exist:
        INSERT

    This makes re-running the same files safe and prevents
    duplicate symbol + Date records.
    """

    merge_query = f"""
        MERGE INTO `{target_table_id}` AS target

        USING (
            SELECT
                SAFE.PARSE_DATE('%Y-%m-%d', Date) AS Date,
                Open,
                High,
                Low,
                Close,
                Adj_Close,
                CAST(ROUND(Volume) AS INT64) AS Volume,

                REGEXP_EXTRACT(
                    _FILE_NAME,
                    r'([^/]+)\\.csv$'
                ) AS symbol

            FROM `{external_table_id}`
        ) AS source

        ON target.symbol = source.symbol
        AND target.Date = source.Date

        WHEN MATCHED THEN
            UPDATE SET
                Open = source.Open,
                High = source.High,
                Low = source.Low,
                Close = source.Close,
                Adj_Close = source.Adj_Close,
                Volume = source.Volume

        WHEN NOT MATCHED THEN
            INSERT (
                Date,
                Open,
                High,
                Low,
                Close,
                Adj_Close,
                Volume,
                symbol
            )

            VALUES (
                source.Date,
                source.Open,
                source.High,
                source.Low,
                source.Close,
                source.Adj_Close,
                source.Volume,
                source.symbol
            )
    """

    query_job = bq_client.query(merge_query)

    query_job.result()

    return query_job.num_dml_affected_rows


# ============================================================
# MERGE METADATA
# ============================================================

def merge_metadata_file(
    gcs_uri: str,
    target_table_id: str
) -> int:
    """
    Loads symbols_valid_meta.csv into bronze_symbol_metadata.

    The source contains Y/N values for fields such as
    Nasdaq_Traded, so those fields are explicitly defined
    as STRING.

    A temporary external table is created for the CSV.

    IMPORTANT:
    The temporary external table is deleted before creation
    to prevent an old autodetected BOOLEAN schema from being
    reused.
    """

    # --------------------------------------------------------
    # Temporary external table ID
    # --------------------------------------------------------

    external_table_id = (
        target_table_id.rsplit(".", 1)[0]
        + "._ext_symbol_metadata"
    )

    logging.info(
        "Preparing metadata external table: %s",
        external_table_id
    )

    # --------------------------------------------------------
    # IMPORTANT FIX
    #
    # Delete old temporary table first.
    #
    # Previously this table could have been created using
    # autodetection, causing Nasdaq_Traded to become BOOLEAN.
    #
    # The source actually contains Y/N values, so we need
    # STRING.
    # --------------------------------------------------------

    bq_client.delete_table(
        external_table_id,
        not_found_ok=True
    )

    # --------------------------------------------------------
    # Create fresh external table
    # --------------------------------------------------------

    table = bigquery.Table(external_table_id)

    external_config = bigquery.ExternalConfig("CSV")

    external_config.source_uris = [gcs_uri]

    external_config.options.skip_leading_rows = 1

    # NEVER use autodetect for this metadata file.
    external_config.autodetect = False

    # --------------------------------------------------------
    # Explicit metadata schema
    # --------------------------------------------------------

    external_config.schema = [
        bigquery.SchemaField(
            "Nasdaq_Traded",
            "STRING"
        ),

        bigquery.SchemaField(
            "Symbol",
            "STRING"
        ),

        bigquery.SchemaField(
            "Security_Name",
            "STRING"
        ),

        bigquery.SchemaField(
            "Listing_Exchange",
            "STRING"
        ),

        bigquery.SchemaField(
            "Market_Category",
            "STRING"
        ),

        bigquery.SchemaField(
            "ETF",
            "STRING"
        ),

        bigquery.SchemaField(
            "Round_Lot_Size",
            "FLOAT64"
        ),

        bigquery.SchemaField(
            "Test_Issue",
            "STRING"
        ),

        bigquery.SchemaField(
            "Financial_Status",
            "STRING"
        ),

        bigquery.SchemaField(
            "CQS_Symbol",
            "STRING"
        ),

        bigquery.SchemaField(
            "NASDAQ_Symbol",
            "STRING"
        ),

        bigquery.SchemaField(
            "NextShares",
            "STRING"
        ),
    ]

    table.external_data_configuration = external_config

    # Create the fresh external table.
    bq_client.create_table(table)

    logging.info(
        "Created external table for metadata file: %s",
        gcs_uri
    )

    # --------------------------------------------------------
    # MERGE metadata into Bronze
    # --------------------------------------------------------

    merge_query = f"""
        MERGE INTO `{target_table_id}` AS target

        USING `{external_table_id}` AS source

        ON target.Symbol = source.Symbol

        WHEN MATCHED THEN
            UPDATE SET
                Nasdaq_Traded = source.Nasdaq_Traded,
                Security_Name = source.Security_Name,
                Listing_Exchange = source.Listing_Exchange,
                Market_Category = source.Market_Category,
                ETF = source.ETF,
                Round_Lot_Size = source.Round_Lot_Size,
                Test_Issue = source.Test_Issue,
                Financial_Status = source.Financial_Status,
                CQS_Symbol = source.CQS_Symbol,
                NASDAQ_Symbol = source.NASDAQ_Symbol,
                NextShares = source.NextShares

        WHEN NOT MATCHED THEN
            INSERT (
                Nasdaq_Traded,
                Symbol,
                Security_Name,
                Listing_Exchange,
                Market_Category,
                ETF,
                Round_Lot_Size,
                Test_Issue,
                Financial_Status,
                CQS_Symbol,
                NASDAQ_Symbol,
                NextShares
            )

            VALUES (
                source.Nasdaq_Traded,
                source.Symbol,
                source.Security_Name,
                source.Listing_Exchange,
                source.Market_Category,
                source.ETF,
                source.Round_Lot_Size,
                source.Test_Issue,
                source.Financial_Status,
                source.CQS_Symbol,
                source.NASDAQ_Symbol,
                source.NextShares
            )
    """

    try:

        query_job = bq_client.query(merge_query)

        query_job.result()

        rows_affected = query_job.num_dml_affected_rows

        logging.info(
            "Metadata MERGE completed successfully: %s rows affected",
            rows_affected
        )

        return rows_affected

    finally:

        # ----------------------------------------------------
        # Always remove temporary external table.
        #
        # This ensures the next run starts clean.
        # ----------------------------------------------------

        bq_client.delete_table(
            external_table_id,
            not_found_ok=True
        )

        logging.info(
            "Dropped temporary metadata external table: %s",
            external_table_id
        )


# ============================================================
# DROP TABLE
# ============================================================

def drop_table(table_id: str):
    """
    Deletes a temporary table.
    """

    bq_client.delete_table(
        table_id,
        not_found_ok=True
    )

    logging.info(
        "Dropped temporary table %s",
        table_id
    )


# ============================================================
# AUDIT
# ============================================================

def write_audit(record: dict):
    """
    Inserts one row into the ingestion_audit table describing
    how a run went.
    """

    errors = bq_client.insert_rows_json(
        AUDIT_TABLE,
        [record]
    )

    if errors:

        logging.error(
            "Failed to write audit row: %s",
            errors
        )

    else:

        logging.info(
            "Audit row written: %s",
            record.get("batch_id")
        )
def write_silver_ready_marker(run_id: str):
    """
    Writes an empty marker file to GCS once Bronze (stock + ETF) has
    fully completed. This upload fires the GCS trigger on the
    bronze-to-silver Cloud Function, which watches for files matching
    silver_trigger/<run_id>/_ready and ignores everything else.

    Call this only after confirming both stock and ETF batches
    succeeded (i.e. right after your existing completion check).
    """

    bucket = storage_client.bucket(BUCKET_NAME)

    marker_path = f"silver_trigger/{run_id}/_ready"

    blob = bucket.blob(marker_path)

    blob.upload_from_string(
        "",
        content_type="text/plain",
    )

    logging.info(
        "Silver trigger marker written: %s",
        marker_path,
    )