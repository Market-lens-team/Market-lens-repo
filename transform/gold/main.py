"""
main.py

GCS-triggered Cloud Function. Fires on every object-finalize event in
the bucket, but only acts on the Gold trigger marker written by the
Silver Cloud Function once Silver has fully completed:

    gold_trigger/<run_id>/_ready

Everything else is ignored immediately.

On trigger:
    1. Calls gold.sp_refresh_all(run_id), which rebuilds every Gold
       table/view and writes its own per-step SUCCESS/FAILED audit
       row to bronze.ingestion_audit (see sp_gold.sql).
    2. Writes one additional "overall" audit row for this Cloud
       Function's own run, so a full pipeline failure (e.g. the
       Cloud Function itself crashing before/after the CALL) is
       still visible even if every individual step inside the
       procedure succeeded.
"""

import logging
from datetime import datetime, timezone

import functions_framework

from config import GOLD_TRIGGER_PREFIX, GOLD_TRIGGER_SUFFIX

from utils import run_gold_refresh, write_gold_audit

logging.basicConfig(level=logging.INFO)


@functions_framework.cloud_event
def silver_to_gold(cloud_event):
    """
    GCS-triggered Gold entry point.

    Silver writes:
        gold_trigger/<run_id>/_ready

    This function ignores all other GCS events, calls the Gold stored
    procedure, and writes the Gold result to the shared audit table.
    """

    data = cloud_event.data
    file_name = data.get("name", "")

    logging.info("GCS event received for: %s", file_name)

    # --------------------------------------------------------
    # ONLY REACT TO: gold_trigger/<run_id>/_ready
    # --------------------------------------------------------

    if not (
        file_name.startswith(GOLD_TRIGGER_PREFIX)
        and file_name.endswith(GOLD_TRIGGER_SUFFIX)
    ):
        logging.info("Ignoring non-trigger file: %s", file_name)
        return

    parts = file_name.split("/")

    if len(parts) != 3:
        logging.error("Unexpected marker path format: %s", file_name)
        return

    run_id = parts[1]

    logging.info("Gold trigger received for run_id=%s", run_id)

    run_started_at = datetime.now(timezone.utc)

    try:

        rows_affected = run_gold_refresh(run_id)

        write_gold_audit(
            batch_id=run_id,
            asset_type="gold_overall",
            status="SUCCESS",
            started_at=run_started_at,
            loaded_row_count=rows_affected,
        )

        logging.info(
            "Gold refresh complete for run_id=%s, "
            "mart_screener rows=%s",
            run_id,
            rows_affected,
        )

    except Exception as exc:

        logging.exception(
            "Gold refresh failed for run_id=%s", run_id
        )

        write_gold_audit(
            batch_id=run_id,
            asset_type="gold_overall",
            status="FAILED",
            started_at=run_started_at,
            error_message=str(exc),
        )

        # Re-raise so Cloud Functions reports this invocation as
        # failed (visible in Cloud Monitoring / retry behavior),
        # matching how bronze_to_silver behaves on failure.
        raise