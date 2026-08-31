import logging
from datetime import datetime, timezone

import functions_framework

from config import SILVER_TRIGGER_PREFIX, SILVER_TRIGGER_SUFFIX

from silver_utils import (
    run_silver_transform,
    refresh_silver_symbol_metadata,
    write_silver_audit,
    write_quarantine_rows,
    ensure_silver_tables_exist,
)

logging.basicConfig(level=logging.INFO)


@functions_framework.cloud_event
def bronze_to_silver(cloud_event):
    """
    GCS-triggered entry point. Fires on every object finalize event in
    the bucket, but only acts when the file is the Silver "_ready"
    marker written by the Bronze orchestrator. All other file events
    are ignored immediately.
    """

    data = cloud_event.data
    file_name = data.get("name", "")

    if not (
        file_name.startswith(SILVER_TRIGGER_PREFIX)
        and file_name.endswith(SILVER_TRIGGER_SUFFIX)
    ):
        # Not our marker file -- ignore (e.g. a stock/etf CSV upload).
        return

    # Expecting a path like: silver_trigger/<run_id>/_ready
    parts = file_name.split("/")
    if len(parts) < 3:
        logging.error("Unexpected marker path format: %s", file_name)
        return

    run_id = parts[1]

    logging.info("Silver trigger received for run_id=%s", run_id)

    run_started_at = datetime.now(timezone.utc)

    try:
        ensure_silver_tables_exist()

        rows_affected = run_silver_transform(run_id)
        write_silver_audit(
            batch_id=f"{run_id}_market_data",
            asset_type="silver_market_data",
            status="SUCCESS",
            started_at=run_started_at,
            loaded_row_count=rows_affected,
        )

        meta_started_at = datetime.now(timezone.utc)
        meta_rows = refresh_silver_symbol_metadata()
        write_silver_audit(
            batch_id=f"{run_id}_symbol_metadata",
            asset_type="silver_symbol_metadata",
            status="SUCCESS",
            started_at=meta_started_at,
            loaded_row_count=meta_rows,
        )

        quarantine_started_at = datetime.now(timezone.utc)
        quarantine_rows = write_quarantine_rows(run_id)
        write_silver_audit(
            batch_id=f"{run_id}_quarantine",
            asset_type="silver_quarantine",
            status="SUCCESS",
            started_at=quarantine_started_at,
            loaded_row_count=quarantine_rows,
        )

    except Exception as exc:
        logging.exception("Silver transform failed for run_id=%s", run_id)
        write_silver_audit(
            batch_id=run_id,
            asset_type="silver",
            status="FAILED",
            started_at=run_started_at,
            error_message=str(exc),
        )
        raise