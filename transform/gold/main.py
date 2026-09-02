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

    data = cloud_event.data or {}
    file_name = data.get("name", "")

    if not (
        file_name.startswith(GOLD_TRIGGER_PREFIX)
        and file_name.endswith(GOLD_TRIGGER_SUFFIX)
    ):
        return

    parts = file_name.split("/")
    if len(parts) != 3:
        logging.error("Unexpected Gold marker path: %s", file_name)
        return

    run_id = parts[1]

    if not run_id:
        logging.error("Gold marker has an empty run_id: %s", file_name)
        return

    logging.info("Gold trigger received for run_id=%s", run_id)

    run_started_at = datetime.now(timezone.utc)

    try:
        rows_affected = run_gold_refresh(run_id)

        write_gold_audit(
            batch_id=f"{run_id}_gold_refresh",
            asset_type="gold",
            status="SUCCESS",
            started_at=run_started_at,
            loaded_row_count=rows_affected,
        )

        logging.info(
            "Gold refresh completed successfully for run_id=%s",
            run_id,
        )

    except Exception as exc:
        logging.exception("Gold refresh failed for run_id=%s", run_id)

        write_gold_audit(
            batch_id=f"{run_id}_gold_refresh",
            asset_type="gold",
            status="FAILED",
            started_at=run_started_at,
            error_message=str(exc),
        )

        raise