import logging
from datetime import datetime, timezone
from pathlib import Path

import functions_framework

from config import (
    BUCKET_NAME,
    STOCK_TABLE,
    ETF_TABLE,
)

from utils import (
    validate_price_file,
    quarantine_file,
    load_price_batch,
    check_duplicate_file,
    write_audit,
)


logging.basicConfig(
    level=logging.INFO
)


def utc_now():

    return datetime.now(
        timezone.utc
    )


@functions_framework.cloud_event
def gcs_to_bronze(
    cloud_event
):

    event = cloud_event.data

    object_name = event["name"]

    logging.info(
        "Received GCS event: %s",
        object_name,
    )

    # ========================================================
    # ONLY CSV
    # ========================================================

    if not object_name.lower().endswith(
        ".csv"
    ):

        logging.info(
            "Ignoring non-CSV object: %s",
            object_name,
        )

        return

    # ========================================================
    # EXPECTED PATH
    #
    # historical/stocks/A.csv
    # historical/etfs/SPY.csv
    # incremental/stocks/AAPL.csv
    # incremental/etfs/QQQ.csv
    # ========================================================

    parts = object_name.split("/")

    if len(parts) != 3:

        logging.info(
            "Ignoring unsupported path: %s",
            object_name,
        )

        return

    load_type = parts[0].upper()

    asset_folder = parts[1].lower()

    file_name = parts[2]

    # ========================================================
    # LOAD TYPE
    # ========================================================

    if load_type not in {
        "HISTORICAL",
        "INCREMENTAL",
    }:

        logging.info(
            "Ignoring unsupported load type: %s",
            load_type,
        )

        return

    # ========================================================
    # ASSET TYPE
    # ========================================================

    if asset_folder not in {
        "stocks",
        "etfs",
    }:

        logging.info(
            "Ignoring unsupported asset folder: %s",
            asset_folder,
        )

        return

    # ========================================================
    # TARGET TABLE
    # ========================================================

    if asset_folder == "stocks":

        target_table = STOCK_TABLE

        asset_type = "STOCK"

    else:

        target_table = ETF_TABLE

        asset_type = "ETF"

    # ========================================================
    # GET BLOB
    # ========================================================

    from google.cloud import storage

    storage_client = storage.Client()

    bucket = storage_client.bucket(
        BUCKET_NAME
    )

    blob = bucket.blob(
        object_name
    )

    blob.reload()

    # ========================================================
    # DUPLICATE CHECK
    # ========================================================

    try:

        already_processed = (
            check_duplicate_file(
                object_name
            )
        )

        if already_processed:

            logging.info(
                "File already processed. "
                "Skipping: %s",
                object_name,
            )

            return

    except Exception as exc:

        logging.exception(
            "Duplicate check failed"
        )

        raise

    # ========================================================
    # VALIDATE
    # ========================================================

    valid, reason = (
        validate_price_file(blob)
    )

    if not valid:

        quarantine_file(
            blob,
            asset_type,
            reason,
        )

        write_audit({

            "batch_id": (
                f"{load_type.lower()}_"
                f"{asset_folder}_"
                f"{Path(file_name).stem}"
            ),

            "load_type": load_type,

            "asset_type": asset_type,

            "gcs_path": object_name,

            "expected_file_count": 1,

            "processed_file_count": 0,

            "expected_row_count": None,

            "loaded_row_count": 0,

            "failed_file_count": 1,

            "status": "QUARANTINED",

            "started_at": (
                utc_now().isoformat()
            ),

            "completed_at": (
                utc_now().isoformat()
            ),

            "error_message": reason,

        })

        return

    # ========================================================
    # LOAD
    # ========================================================

    started_at = utc_now()

    symbol = Path(
        file_name
    ).stem.upper()

    external_table_name = (
        f"_ext_"
        f"{asset_folder}_"
        f"{symbol}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    )

    try:

        rows = load_price_batch(

            uris=[
                f"gs://{BUCKET_NAME}/"
                f"{object_name}"
            ],

            target_table=target_table,

            external_table_name=(
                external_table_name
            ),
        )

        # ====================================================
        # AUDIT SUCCESS
        # ====================================================

        write_audit({

            "batch_id": (
                f"{load_type.lower()}_"
                f"{asset_folder}_"
                f"{symbol}"
            ),

            "load_type": load_type,

            "asset_type": asset_type,

            "gcs_path": object_name,

            "expected_file_count": 1,

            "processed_file_count": 1,

            "expected_row_count": None,

            "loaded_row_count": rows,

            "failed_file_count": 0,

            "status": "SUCCESS",

            "started_at": (
                started_at.isoformat()
            ),

            "completed_at": (
                utc_now().isoformat()
            ),

            "error_message": None,

        })

        logging.info(
            "SUCCESS: %s | Rows=%s",
            object_name,
            rows,
        )

    except Exception as exc:

        logging.exception(
            "File load failed: %s",
            object_name,
        )

        write_audit({

            "batch_id": (
                f"{load_type.lower()}_"
                f"{asset_folder}_"
                f"{symbol}"
            ),

            "load_type": load_type,

            "asset_type": asset_type,

            "gcs_path": object_name,

            "expected_file_count": 1,

            "processed_file_count": 0,

            "expected_row_count": None,

            "loaded_row_count": 0,

            "failed_file_count": 1,

            "status": "FAILED",

            "started_at": (
                started_at.isoformat()
            ),

            "completed_at": (
                utc_now().isoformat()
            ),

            "error_message": str(exc)[
                :5000
            ],

        })

        raise