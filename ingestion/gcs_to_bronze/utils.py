import csv
import io
import logging
from pathlib import Path

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage

from config import (
    PROJECT_ID,
    REGION,
    BUCKET_NAME,
    BRONZE_DATASET,
    AUDIT_TABLE,
    QUARANTINE_PREFIX,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)


# ============================================================
# CLIENTS
# ============================================================

storage_client = storage.Client(
    project=PROJECT_ID
)

bq_client = bigquery.Client(
    project=PROJECT_ID
)


# ============================================================
# EXPECTED CSV HEADER
# ============================================================
#
# This MUST match the actual CSV header.
#
# Actual CSV:
#
# Date,Open,High,Low,Close,Adj Close,Volume
#
# IMPORTANT:
# "Adj Close" is only the CSV header.
# BigQuery external table field will be "Adj_Close".
# ============================================================

PRICE_HEADERS = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Adj Close",
    "Volume",
]


# ============================================================
# LIST CSV FILES
# ============================================================

def list_csv_files(prefix: str):
    """
    List all non-empty CSV files under a GCS prefix.
    """

    blobs = storage_client.list_blobs(
        BUCKET_NAME,
        prefix=prefix,
    )

    return [
        blob
        for blob in blobs
        if blob.name.lower().endswith(".csv")
        and blob.size
        and blob.size > 0
    ]


# ============================================================
# READ CSV HEADER
# ============================================================

def read_header(blob):
    """
    Read the first line of a CSV file from GCS.
    """

    raw = blob.download_as_bytes(
        start=0,
        end=8191,
    )

    text = raw.decode(
        "utf-8-sig",
        errors="replace",
    )

    lines = text.splitlines()

    if not lines:
        return []

    return next(
        csv.reader(
            io.StringIO(lines[0])
        )
    )


# ============================================================
# VALIDATE PRICE FILE
# ============================================================

def validate_price_file(blob):
    """
    Validate that the CSV header exactly matches
    the expected stock/ETF price structure.
    """

    try:

        actual = [
            value.strip()
            for value in read_header(blob)
        ]

        if actual != PRICE_HEADERS:

            return (
                False,
                f"HEADER_MISMATCH: {actual}",
            )

        return True, None

    except Exception as exc:

        return (
            False,
            f"HEADER_READ_ERROR: {exc}",
        )


# ============================================================
# QUARANTINE
# ============================================================

def quarantine_file(
    blob,
    asset_type,
    reason,
):
    """
    Copy an invalid CSV into the quarantine prefix.

    Example:

    historical/stocks/A.csv

    becomes:

    quarantine/stock/A.csv
    """

    bucket = storage_client.bucket(
        BUCKET_NAME
    )

    destination = (
        f"{QUARANTINE_PREFIX}/"
        f"{asset_type.lower()}/"
        f"{Path(blob.name).name}"
    )

    bucket.copy_blob(
        blob,
        bucket,
        destination,
    )

    logging.error(
        "Quarantined %s because %s",
        blob.name,
        reason,
    )


# ============================================================
# CREATE EXTERNAL TABLE
# ============================================================

def create_external_table(
    external_table_name,
    uris,
):
    """
    Create a temporary BigQuery external table over
    one or more CSV files in GCS.

    CSV header:

        Date,Open,High,Low,Close,Adj Close,Volume

    External BigQuery schema:

        Date
        Open
        High
        Low
        Close
        Adj_Close
        Volume

    The CSV header and BigQuery field names do NOT have
    to be identical because skip_leading_rows = 1 is used.
    """

    table_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"{external_table_name}"
    )

    # --------------------------------------------------------
    # External table schema
    # --------------------------------------------------------
    #
    # IMPORTANT:
    #
    # DO NOT use:
    #
    #     "Adj Close"
    #
    # because BigQuery field names cannot contain spaces.
    #
    # Use:
    #
    #     "Adj_Close"
    #
    # instead.
    #
    # The CSV itself still contains:
    #
    #     Adj Close
    # --------------------------------------------------------

    source_schema = [

        bigquery.SchemaField(
            "Date",
            "STRING",
        ),

        bigquery.SchemaField(
            "Open",
            "STRING",
        ),

        bigquery.SchemaField(
            "High",
            "STRING",
        ),

        bigquery.SchemaField(
            "Low",
            "STRING",
        ),

        bigquery.SchemaField(
            "Close",
            "STRING",
        ),

        bigquery.SchemaField(
            "Adj_Close",
            "STRING",
        ),

        bigquery.SchemaField(
            "Volume",
            "STRING",
        ),
    ]

    # --------------------------------------------------------
    # External configuration
    # --------------------------------------------------------

    external_config = bigquery.ExternalConfig(
        bigquery.ExternalSourceFormat.CSV
    )

    external_config.source_uris = uris

    # Skip:

    # Date,Open,High,...

    external_config.options.skip_leading_rows = 1

    external_config.schema = source_schema

    # --------------------------------------------------------
    # Create BigQuery table object
    # --------------------------------------------------------

    table = bigquery.Table(
        table_id,
        schema=source_schema,
    )

    table.external_data_configuration = (
        external_config
    )

    # --------------------------------------------------------
    # Retry configuration
    # --------------------------------------------------------

    create_retry = retry.Retry(
        initial=2.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=120,
    )

    # --------------------------------------------------------
    # Create external table
    # --------------------------------------------------------

    create_retry(
        bq_client.create_table
    )(
        table,
        exists_ok=True,
    )

    logging.info(
        "Created external table: %s",
        table_id,
    )

    return table_id


# ============================================================
# DELETE EXTERNAL TABLE
# ============================================================

def delete_external_table(
    table_id,
):
    """
    Delete temporary external table.
    """

    bq_client.delete_table(
        table_id,
        not_found_ok=True,
    )

    logging.info(
        "Deleted external table: %s",
        table_id,
    )


# ============================================================
# LOAD PRICE BATCH
# ============================================================
#
# This function can load one CSV or multiple CSVs.
#
# For the new Cloud Function architecture:
#
#     one invocation -> one CSV
#
# For backfill:
#
#     one invocation can still call this function
#     with one URI.
# ============================================================

def load_price_batch(
    uris,
    target_table,
    external_table_name,
):
    """
    Load CSV files from GCS into Bronze BigQuery table.
    """

    external_table = create_external_table(
        external_table_name,
        uris,
    )

    target_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"{target_table}"
    )

    # --------------------------------------------------------
    # Insert into Bronze
    # --------------------------------------------------------

    query = f"""

    INSERT INTO `{target_id}`
    (
        Date,
        symbol,
        Open,
        High,
        Low,
        Close,
        Adj_Close,
        Volume
    )

    SELECT

        COALESCE(

            SAFE.PARSE_DATE(
                '%Y-%m-%d',
                Date
            ),

            SAFE.PARSE_DATE(
                '%d-%m-%Y',
                Date
            ),

            SAFE.PARSE_DATE(
                '%m/%d/%Y',
                Date
            )

        ) AS Date,

        REGEXP_EXTRACT(
            _FILE_NAME,
            r'/([^/]+)\\.csv$'
        ) AS symbol,

        SAFE_CAST(
            Open AS FLOAT64
        ) AS Open,

        SAFE_CAST(
            High AS FLOAT64
        ) AS High,

        SAFE_CAST(
            Low AS FLOAT64
        ) AS Low,

        SAFE_CAST(
            Close AS FLOAT64
        ) AS Close,

        SAFE_CAST(
            Adj_Close AS FLOAT64
        ) AS Adj_Close,

        SAFE_CAST(
            Volume AS FLOAT64
        ) AS Volume

    FROM `{external_table}`

    WHERE COALESCE(

        SAFE.PARSE_DATE(
            '%Y-%m-%d',
            Date
        ),

        SAFE.PARSE_DATE(
            '%d-%m-%Y',
            Date
        ),

        SAFE.PARSE_DATE(
            '%m/%d/%Y',
            Date
        )

    ) IS NOT NULL

    """

    try:

        logging.info(
            "Loading data into Bronze table: %s",
            target_table,
        )

        logging.info(
            "External table: %s",
            external_table,
        )

        logging.info(
            "Number of GCS files: %s",
            len(uris),
        )

        # ----------------------------------------------------
        # Execute query
        # ----------------------------------------------------

        job = bq_client.query(
            query,
            location=REGION,
        )

        job.result()

        rows_loaded = (
            job.num_dml_affected_rows
            or 0
        )

        logging.info(
            "Batch loaded successfully. "
            "Rows inserted: %s",
            rows_loaded,
        )

        return rows_loaded

    finally:

        # ----------------------------------------------------
        # Always delete temporary external table
        # ----------------------------------------------------

        delete_external_table(
            external_table
        )


# ============================================================
# CHECK DUPLICATE FILE
# ============================================================

def check_duplicate_file(gcs_path):

    table_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"{AUDIT_TABLE}"
    )

    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{table_id}`
        WHERE gcs_path = @gcs_path
          AND status = 'SUCCESS'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "gcs_path",
                "STRING",
                gcs_path,
            )
        ]
    )

    query_job = bq_client.query(
        query,
        job_config=job_config,
        location=REGION,
    )

    result = list(query_job.result())

    return bool(result and result[0].cnt > 0)

# ============================================================
# WRITE AUDIT
# ============================================================

def write_audit(
    record,
):
    """
    Write ingestion result into BigQuery audit table.
    """

    table_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"{AUDIT_TABLE}"
    )

    try:

        insert_retry = retry.Retry(
            initial=2.0,
            maximum=30.0,
            multiplier=2.0,
            deadline=120,
        )

        insert_rows = insert_retry(
            bq_client.insert_rows_json
        )

        errors = insert_rows(
            table_id,
            [record],
        )

        if errors:

            logging.error(
                "Audit insert failed: %s",
                errors,
            )

        else:

            logging.info(
                "Audit record inserted successfully"
            )

    except Exception as exc:

        logging.exception(
            "Audit write failed: %s",
            exc,
        )

        # Audit failure should be visible in logs.
        # Re-raise so the caller knows that audit failed.
        raise