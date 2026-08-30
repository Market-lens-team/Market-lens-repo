import logging
from datetime import datetime, timezone

import functions_framework

from silver_utils import (
    run_silver_transform,
    refresh_silver_symbol_metadata,
    write_silver_audit,
    ensure_silver_tables_exist,
)

logging.basicConfig(level=logging.INFO)

# Only react to the marker file placed under this prefix once Bronze
# (stock + ETF) has finished successfully.
SILVER_TRIGGER_PREFIX = "silver_trigger/"
SILVER_TRIGGER_SUFFIX = "_ready"


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

    # Capture Silver execution start time for audit
    started_at = datetime.now(timezone.utc)

    logging.info("Silver trigger received for run_id=%s", run_id)

    try:
        ensure_silver_tables_exist()

        rows_affected = run_silver_transform(run_id)

        refresh_silver_symbol_metadata()

        write_silver_audit(
            batch_id=run_id,
            asset_type="ALL",
            status="SUCCESS",
            started_at=started_at,
            loaded_row_count=rows_affected
        )

    except Exception as exc:
        logging.exception(
            "Silver transform failed for run_id=%s",
            run_id
        )

        write_silver_audit(
            batch_id=run_id,
            asset_type="ALL",
            status="FAILED",
            started_at=started_at,
            loaded_row_count=0,
            error_message=str(exc)
        )

        raise