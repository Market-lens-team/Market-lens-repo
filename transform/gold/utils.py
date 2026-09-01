import logging
from datetime import datetime, timezone

from google.api_core import retry
from google.cloud import bigquery

from config import PROJECT_ID, REGION, GOLD_DATASET, AUDIT_DATASET, AUDIT_TABLE

logging.basicConfig(level=logging.INFO)

bq_client = bigquery.Client(project=PROJECT_ID)


def run_gold_refresh() -> int:
    """
    Calls the single Gold stored procedure that rebuilds every
    dimension, fact, and mart table in dependency order.

    A CALL statement isn't DML, so BigQuery's num_dml_affected_rows
    doesn't reflect real row counts here. Instead, after the
    procedure finishes, this queries mart_screener's row count
    directly as a simple, real health signal for the audit log.
    """

    call_query = f"CALL {PROJECT_ID}.{GOLD_DATASET}.sp_refresh_all()"

    query_retry = retry.Retry(initial=2.0, maximum=30.0, multiplier=2.0, deadline=480)

    logging.info("Starting Gold refresh (CALL sp_refresh_all)")

    job = bq_client.query(call_query, location=REGION)
    query_retry(job.result)()

    logging.info("Gold refresh procedure completed")

    count_query = (
        f"SELECT COUNT(*) AS row_count "
        f"FROM {PROJECT_ID}.{GOLD_DATASET}.mart_screener"
    )
    count_job = bq_client.query(count_query, location=REGION)
    result = list(count_job.result())
    rows_affected = result[0].row_count if result else 0

    logging.info("Gold refresh row count (mart_screener): %s", rows_affected)

    return rows_affected


def write_gold_audit(
    batch_id: str,
    asset_type: str,
    status: str,
    started_at,
    loaded_row_count: int = 0,
    error_message: str = None,
):
    """
    Logs a Gold run to the shared ingestion_audit table, same shape
    and pattern as Bronze/Silver's audit writes, with load_type='GOLD'.
    """

    table_id = f"{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE}"

    record = {
        "batch_id": batch_id,
        "load_type": "GOLD",
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
        logging.error("Gold audit insert failed: %s", errors)
    else:
        logging.info("Gold audit record written: %s", batch_id)