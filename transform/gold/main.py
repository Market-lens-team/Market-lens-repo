import logging
from datetime import datetime, timezone

import functions_framework

from config import GOLD_TRIGGER_PREFIX, GOLD_TRIGGER_SUFFIX

from utils import (
    run_gold_refresh,
    write_gold_audit,
)

logging.basicConfig(level=logging.INFO)


@functions_framework.cloud_event
def silver_to_gold(cloud_event):
    """
    GCS-triggered entry point. Fires on every object finalize event in
    the bucket, but only acts when the file is the Gold "_ready" marker
    written by the Silver stage. All other file events are ignored
    immediately.

    Expecting a path like: gold_trigger/<run_id>/_ready
    (mirrors exactly how Bronze triggers Silver via silver_trigger/<run_id>/_ready)
    """

    data = cloud_event.data
    file_name = data.get("name", "")

    if not (
        file_name.startswith(GOLD_TRIGGER_PREFIX)
        and file_name.endswith(GOLD_TRIGGER_SUFFIX)
    ):
        # Not our marker file -- ignore (e.g. a stock/etf CSV upload,
        # or the silver_trigger marker meant for the Silver function).
        return

    parts = file_name.split("/")
    if len(parts) < 3:
        logging.error("Unexpected marker path format: %s", file_name)
        return

    run_id = parts[1]

    logging.info("Gold trigger received for run_id=%s", run_id)

    run_started_at = datetime.now(timezone.utc)

    try:
        # runs CALL gold.sp_refresh_all() and returns however many
        # rows the procedure's tables ended up with, for the audit log
        rows_affected = run_gold_refresh()

        write_gold_audit(
            batch_id=f"{run_id}_gold_refresh",
            asset_type="gold",
            status="SUCCESS",
            started_at=run_started_at,
            loaded_row_count=rows_affected,
        )

        logging.info(
            "Gold refresh completed successfully for run_id=%s", run_id
        )

    except Exception as exc:
        logging.exception("Gold refresh failed for run_id=%s", run_id)

        write_gold_audit(
            batch_id=run_id,
            asset_type="gold",
            status="FAILED",
            started_at=run_started_at,
            error_message=str(exc),
        )

        raise