import logging
from datetime import datetime, timezone

import functions_framework

from config import (
    STOCK_BATCH_SIZE,
    ETF_BATCH_SIZE,
    STOCK_TABLE,
    ETF_TABLE,
)

from utils import (
    list_csv_files,
    validate_price_file,
    quarantine_file,
    load_price_batch,
    write_audit,
)

logging.basicConfig(level=logging.INFO)


def utc_now():
    return datetime.now(timezone.utc)


def chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index:index + size]


@functions_framework.cloud_event
def gcs_to_bronze(cloud_event):

    event = cloud_event.data

    object_name = event["name"]
    generation = str(event.get("generation", ""))

    logging.info(
        "Received GCS event. Object=%s Generation=%s",
        object_name,
        generation,
    )

    if not object_name.endswith("/_READY"):
        logging.info(
            "Ignoring normal file upload: %s",
            object_name,
        )
        return

    parts = object_name.split("/")

    if len(parts) != 3:
        logging.error(
            "Invalid READY path: %s",
            object_name,
        )
        return

    load_type = parts[0].upper()
    asset_folder = parts[1].lower()

    if load_type not in {
        "HISTORICAL",
        "INCREMENTAL",
    }:
        logging.error(
            "Invalid load type: %s",
            load_type,
        )
        return

    if asset_folder not in {
        "stocks",
        "etfs",
    }:
        logging.error(
            "Invalid asset folder: %s",
            asset_folder,
        )
        return

    prefix = f"{parts[0]}/{parts[1]}/"

    if asset_folder == "stocks":
        target_table = STOCK_TABLE
        asset_type = "STOCK"
        batch_size = STOCK_BATCH_SIZE
    else:
        target_table = ETF_TABLE
        asset_type = "ETF"
        batch_size = ETF_BATCH_SIZE

    started_at = utc_now()

    batch_id = (
        f"{load_type.lower()}_"
        f"{asset_folder}_"
        f"{generation}"
    )

    blobs = list_csv_files(prefix)

    expected_files = len(blobs)

    if expected_files == 0:

        write_audit({
            "batch_id": batch_id,
            "load_type": load_type,
            "asset_type": asset_type,
            "gcs_path": prefix,
            "expected_file_count": 0,
            "processed_file_count": 0,
            "expected_row_count": None,
            "loaded_row_count": 0,
            "failed_file_count": 0,
            "status": "FAILED",
            "started_at": started_at.isoformat(),
            "completed_at": utc_now().isoformat(),
            "error_message": "No CSV files found",
        })

        return

    valid_files = []
    failed_files = 0

    for blob in blobs:

        valid, reason = validate_price_file(blob)

        if not valid:

            failed_files += 1

            quarantine_file(
                blob,
                asset_folder,
                reason,
            )

            continue

        valid_files.append(blob)

    processed_files = 0
    loaded_rows = 0

    try:

        for batch_number, batch in enumerate(
            chunks(valid_files, batch_size),
            start=1,
        ):
            logging.info(
                "Processing batch %s with %s files",
                batch_number,
                len(batch),
            )

            uris = [
                f"gs://{blob.bucket.name}/{blob.name}"
                for blob in batch
            ]

            external_table_name = (
                f"_ext_"
                f"{asset_folder}_"
                f"{generation}_"
                f"{batch_number}"
            )

            rows = load_price_batch(
                uris=uris,
                target_table=target_table,
                external_table_name=external_table_name,
            )

            loaded_rows += rows
            processed_files += len(batch)

        status = (
            "SUCCESS"
            if failed_files == 0
            else "PARTIAL"
        )

        write_audit({
            "batch_id": batch_id,
            "load_type": load_type,
            "asset_type": asset_type,
            "gcs_path": prefix,
            "expected_file_count": expected_files,
            "processed_file_count": processed_files,
            "expected_row_count": None,
            "loaded_row_count": loaded_rows,
            "failed_file_count": failed_files,
            "status": status,
            "started_at": started_at.isoformat(),
            "completed_at": utc_now().isoformat(),
            "error_message": (
                None
                if failed_files == 0
                else f"{failed_files} files quarantined"
            ),
        })

    except Exception as exc:
        logging.exception("Batch failed")

        try:
            write_audit({
                "batch_id": batch_id,
                "load_type": load_type,
                "asset_type": asset_type,
                "gcs_path": prefix,
                "expected_file_count": expected_files,
                "processed_file_count": processed_files,
                "expected_row_count": None,
                "loaded_row_count": loaded_rows,
                "failed_file_count": failed_files,
                "status": "FAILED",
                "started_at": started_at.isoformat(),
                "completed_at": utc_now().isoformat(),
                "error_message": str(exc)[:5000],
            })

        except Exception as audit_exc:

            logging.exception(
                "Audit write failed: %s",
                audit_exc,
            )

        raise