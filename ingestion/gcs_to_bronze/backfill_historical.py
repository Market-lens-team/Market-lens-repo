import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import storage

from config import (
    PROJECT_ID,
    REGION,
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
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


storage_client = storage.Client(
    project=PROJECT_ID
)


# ============================================================
# CURRENT UTC TIME
# ============================================================

def utc_now():

    return datetime.now(
        timezone.utc
    )


# ============================================================
# PROCESS ONE FILE
# ============================================================

def process_file(
    blob,
    asset_type,
):

    object_name = blob.name

    logging.info(
        "=================================================="
    )

    logging.info(
        "Processing: %s",
        object_name,
    )

    logging.info(
        "Asset type: %s",
        asset_type,
    )

    # --------------------------------------------------------
    # DUPLICATE CHECK
    # --------------------------------------------------------

    already_processed = check_duplicate_file(
        object_name
    )

    if already_processed:

        logging.info(
            "SKIPPING already processed file: %s",
            object_name,
        )

        return {
            "status": "SKIPPED",
            "rows": 0,
        }

    # --------------------------------------------------------
    # VALIDATE FILE
    # --------------------------------------------------------

    valid, reason = validate_price_file(
        blob
    )

    if not valid:

        logging.error(
            "Invalid file: %s | Reason: %s",
            object_name,
            reason,
        )

        quarantine_file(
            blob,
            asset_type,
            reason,
        )

        return {
            "status": "QUARANTINED",
            "rows": 0,
        }

    # --------------------------------------------------------
    # SELECT TARGET TABLE
    # --------------------------------------------------------

    if asset_type == "STOCK":

        target_table = STOCK_TABLE

    else:

        target_table = ETF_TABLE

    # --------------------------------------------------------
    # LOAD ONE FILE
    # --------------------------------------------------------

    started_at = utc_now()

    file_name = Path(
        object_name
    ).name

    symbol = Path(
        file_name
    ).stem.upper()

    try:

        rows = load_price_batch(
            uris=[
                f"gs://{BUCKET_NAME}/{object_name}"
            ],
            target_table=target_table,
            external_table_name=(
                f"_ext_backfill_{asset_type.lower()}_{symbol}"
            ),
        )

        completed_at = utc_now()

        # ----------------------------------------------------
        # AUDIT SUCCESS
        # ----------------------------------------------------

        write_audit({

            "batch_id": (
                f"historical_"
                f"{asset_type.lower()}_"
                f"{symbol}"
            ),

            "load_type": "HISTORICAL",

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
                completed_at.isoformat()
            ),

            "error_message": None,

        })

        logging.info(
            "SUCCESS: %s | Rows loaded: %s",
            symbol,
            rows,
        )

        return {
            "status": "SUCCESS",
            "rows": rows,
        }

    except Exception as exc:

        logging.exception(
            "FAILED: %s",
            object_name,
        )

        try:

            write_audit({

                "batch_id": (
                    f"historical_"
                    f"{asset_type.lower()}_"
                    f"{symbol}"
                ),

                "load_type": "HISTORICAL",

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

        except Exception:

            logging.exception(
                "Could not write failure audit"
            )

        raise


# ============================================================
# FIND FILE
# ============================================================

def get_file(
    asset,
    symbol,
):

    asset_folder = (
        "stocks"
        if asset == "stocks"
        else "etfs"
    )

    object_name = (
        f"historical/"
        f"{asset_folder}/"
        f"{symbol.upper()}.csv"
    )

    bucket = storage_client.bucket(
        BUCKET_NAME
    )

    blob = bucket.blob(
        object_name
    )

    if not blob.exists():

        raise FileNotFoundError(
            f"GCS file not found: "
            f"gs://{BUCKET_NAME}/{object_name}"
        )

    blob.reload()

    return blob


# ============================================================
# PROCESS ALL FILES
# ============================================================

def process_all(
    asset,
):

    asset_folder = (
        "stocks"
        if asset == "stocks"
        else "etfs"
    )

    asset_type = (
        "STOCK"
        if asset == "stocks"
        else "ETF"
    )

    prefix = (
        f"historical/"
        f"{asset_folder}/"
    )

    logging.info(
        "Starting full historical backfill"
    )

    logging.info(
        "Prefix: %s",
        prefix,
    )

    blobs = storage_client.list_blobs(
        BUCKET_NAME,
        prefix=prefix,
    )

    total = 0
    success = 0
    skipped = 0
    quarantined = 0
    failed = 0
    total_rows = 0

    for blob in blobs:

        if not blob.name.lower().endswith(
            ".csv"
        ):

            continue

        if not blob.size:

            continue

        total += 1

        try:

            result = process_file(
                blob,
                asset_type,
            )

            status = result["status"]

            total_rows += result["rows"]

            if status == "SUCCESS":

                success += 1

            elif status == "SKIPPED":

                skipped += 1

            elif status == "QUARANTINED":

                quarantined += 1

        except Exception:

            failed += 1

    logging.info(
        "=================================================="
    )

    logging.info(
        "HISTORICAL BACKFILL COMPLETE"
    )

    logging.info(
        "Total files: %s",
        total,
    )

    logging.info(
        "Successful: %s",
        success,
    )

    logging.info(
        "Skipped: %s",
        skipped,
    )

    logging.info(
        "Quarantined: %s",
        quarantined,
    )

    logging.info(
        "Failed: %s",
        failed,
    )

    logging.info(
        "Total rows loaded: %s",
        total_rows,
    )

    logging.info(
        "=================================================="
    )


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "MarketLens Historical "
            "GCS → Bronze Backfill"
        )
    )

    parser.add_argument(
        "--asset",
        required=True,
        choices=[
            "stocks",
            "etfs",
        ],
        help="stocks or etfs",
    )

    parser.add_argument(
        "--symbol",
        required=False,
        help=(
            "Process only one symbol. "
            "Example: A"
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # SINGLE SYMBOL TEST
    # --------------------------------------------------------

    if args.symbol:

        blob = get_file(
            args.asset,
            args.symbol,
        )

        asset_type = (
            "STOCK"
            if args.asset == "stocks"
            else "ETF"
        )

        result = process_file(
            blob,
            asset_type,
        )

        logging.info(
            "Test result: %s",
            result,
        )

        return

    # --------------------------------------------------------
    # FULL BACKFILL
    # --------------------------------------------------------

    process_all(
        args.asset
    )


if __name__ == "__main__":

    main()