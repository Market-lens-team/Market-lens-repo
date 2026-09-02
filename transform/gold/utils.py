import logging
from datetime import datetime, timezone

from google.api_core import retry
from google.cloud import bigquery

from config import (
    PROJECT_ID,
    REGION,
    GOLD_DATASET,
    AUDIT_DATASET,
    AUDIT_TABLE,
)

logging.basicConfig(level=logging.INFO)

bq_client = bigquery.Client(project=PROJECT_ID)


# ============================================================
# GOLD STORED PROCEDURE
# ============================================================

def run_gold_refresh(run_id: str) -> int:
    """
    Executes the Gold stored procedure.

    IMPORTANT:
    sp_refresh_all() takes ZERO arguments.

    run_id is used only for:
        - logging
        - audit tracking

    It is NOT passed to BigQuery procedure.
    """

    # --------------------------------------------------------
    # IMPORTANT:
    # Procedure takes ZERO arguments
    # --------------------------------------------------------

    call_query = (
        f"CALL `{PROJECT_ID}.{GOLD_DATASET}.sp_refresh_all`();"
    )

    query_retry = retry.Retry(
        initial=2.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=480,
    )

    logging.info(
        "Starting Gold stored procedure. run_id=%s",
        run_id,
    )

    try:
        job = bq_client.query(
            call_query,
            location=REGION,
        )

        query_retry(job.result)()

        logging.info(
            "Gold stored procedure completed successfully. "
            "run_id=%s",
            run_id,
        )

    except Exception:
        logging.exception(
            "Gold stored procedure failed. run_id=%s",
            run_id,
        )
        raise

    # --------------------------------------------------------
    # Get Gold row count
    # --------------------------------------------------------

    count_query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{PROJECT_ID}.{GOLD_DATASET}.mart_screener`
    """

    logging.info(
        "Getting Gold row count. run_id=%s",
        run_id,
    )

    count_job = bq_client.query(
        count_query,
        location=REGION,
    )

    count_result = list(
        count_job.result()
    )

    rows_affected = (
        int(count_result[0].row_count)
        if count_result
        else 0
    )

    logging.info(
        "Gold mart_screener row count=%s. run_id=%s",
        rows_affected,
        run_id,
    )

    return rows_affected


# ============================================================
# GOLD AUDIT
# ============================================================

def write_gold_audit(
    batch_id: str,
    asset_type: str,
    status: str,
    started_at,
    loaded_row_count: int = 0,
    error_message: str = None,
):
    """
    Writes the overall Gold execution status
    to ingestion_audit.

    This records whether the complete Gold procedure
    succeeded or failed.
    """

    table_id = (
        f"{PROJECT_ID}."
        f"{AUDIT_DATASET}."
        f"{AUDIT_TABLE}"
    )

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

        "completed_at": (
            datetime.now(timezone.utc)
            .isoformat()
        ),

        "error_message": (
            error_message[:5000]
            if error_message
            else None
        ),
    }

    logging.info(
        "Writing Gold audit. "
        "batch_id=%s, status=%s",
        batch_id,
        status,
    )

    insert_retry = retry.Retry(
        initial=2.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=120,
    )

    try:

        insert_rows = insert_retry(
            bq_client.insert_rows_json
        )

        errors = insert_rows(
            table_id,
            [record],
        )

        if errors:
            logging.error(
                "Gold audit insert failed: %s",
                errors,
            )

            raise RuntimeError(
                f"Failed to write ingestion_audit: {errors}"
            )

        logging.info(
            "Gold audit record written successfully. "
            "batch_id=%s",
            batch_id,
        )

    except Exception:
        logging.exception(
            "Failed to write Gold audit. "
            "batch_id=%s",
            batch_id,
        )
        raise