
"""
main.py

GCS-triggered Cloud Function. Ignores every file EXCEPT one named
exactly "_READY" - that marker signals "all data files for this load
have finished uploading, go ahead and process them."

Expected GCS layout:
    historical/
    ├── stocks/*.csv
    ├── etfs/*.csv
    ├── symbols_valid_meta.csv
    └── _READY                <- upload this LAST, after everything above

    incremental/
    ├── stocks/*.csv
    ├── etfs/*.csv
    ├── symbols_valid_meta.csv (optional, only if metadata changed)
    └── _READY                <- upload this LAST

When _READY lands, this function:
    1. Loads symbols_valid_meta.csv -> bronze_symbol_metadata (MERGE)
    2. Loads all stocks/*.csv -> bronze_stock_prices (MERGE, batched)
    3. Loads all etfs/*.csv -> bronze_etf_prices (MERGE, batched)

MERGE means re-uploading the same files and re-triggering is safe -
existing symbol+Date rows get updated, new ones get inserted, no
duplicates are ever created.

AUDIT:
    - Metadata gets one audit row because it is one file.
    - Every stocks batch gets its own audit row.
    - Every ETFs batch gets its own audit row.
    - If a batch fails, that specific batch gets a FAILED audit row.
"""

import logging
from datetime import datetime, timezone

import functions_framework

from config import (
    BUCKET_NAME,
    STOCK_BATCH_SIZE,
    ETF_BATCH_SIZE,
    STOCK_TABLE,
    ETF_TABLE,
    METADATA_TABLE,
    ASSET_TYPES,
    METADATA_FILENAME,
    READY_MARKER_FILENAME,
)

from utils import (
    list_csv_files,
    chunk_list,
    create_external_table,
    get_row_count,
    merge_batch_with_symbol,
    merge_metadata_file,
    drop_table,
    write_audit,
    file_exists,
    PRICE_FILE_SCHEMA,
    write_silver_ready_marker,
)

logging.basicConfig(level=logging.INFO)


def utc_now():
    return datetime.now(timezone.utc)


def process_asset_type(
    load_type: str,
    asset_type: str,
    target_table: str,
    batch_size: int
):
    """
    Runs the batched MERGE-load for one asset type.

    Each batch gets its own audit row.

    Example:

        stocks batch 1 -> audit row
        stocks batch 2 -> audit row
        stocks batch 3 -> audit row

        ETFs batch 1 -> audit row
        ETFs batch 2 -> audit row

    This allows the ingestion_audit table to show exactly
    which batch succeeded or failed.
    """

    prefix = f"{load_type.lower()}/{asset_type}/"

    started_at = utc_now()

    run_id = (
        f"{load_type.lower()}_{asset_type}_"
        f"{started_at.strftime('%Y%m%d%H%M%S')}"
    )

    logging.info(
        "Starting %s -> prefix=%s target=%s",
        run_id,
        prefix,
        target_table
    )

    all_uris = list_csv_files(prefix)

    expected_file_count = len(all_uris)

    # ========================================================
    # NO FILES FOUND
    # ========================================================

    if expected_file_count == 0:

        write_audit({
            "batch_id": run_id,
            "load_type": load_type,
            "asset_type": asset_type,
            "gcs_path": prefix,
            "expected_file_count": 0,
            "processed_file_count": 0,
            "expected_row_count": 0,
            "loaded_row_count": 0,
            "status": "FAILED",
            "started_at": started_at.isoformat(),
            "completed_at": utc_now().isoformat(),
            "error_message": "No CSV files found at this path",
        })

        return

    # ========================================================
    # TOTAL RUN COUNTERS
    # ========================================================

    total_expected_rows = 0
    total_loaded_rows = 0
    processed_files = 0
    batch_number = 0

    # ========================================================
    # PROCESS EACH BATCH
    # ========================================================

    for batch_uris in chunk_list(all_uris, batch_size):

        batch_number += 1

        batch_started_at = utc_now()

        # Unique audit ID for every batch
        batch_id = (
            f"{run_id}_batch_{batch_number}"
        )

        external_table_id = (
            f"{target_table.rsplit('.', 1)[0]}."
            f"_ext_{asset_type}_{run_id}_{batch_number}"
        )

        logging.info(
            "Starting %s: %s files",
            batch_id,
            len(batch_uris)
        )

        expected_rows_this_batch = 0
        loaded_rows_this_batch = 0

        try:

            # ------------------------------------------------
            # CREATE EXTERNAL TABLE
            # ------------------------------------------------

            create_external_table(
                batch_uris,
                external_table_id,
                PRICE_FILE_SCHEMA
            )

            # ------------------------------------------------
            # COUNT SOURCE ROWS
            # ------------------------------------------------

            expected_rows_this_batch = get_row_count(
                external_table_id
            )

            logging.info(
                "%s source row count: %s",
                batch_id,
                expected_rows_this_batch
            )

            # ------------------------------------------------
            # MERGE INTO BRONZE
            # ------------------------------------------------

            loaded_rows_this_batch = merge_batch_with_symbol(
                external_table_id,
                target_table
            )

            # ------------------------------------------------
            # DROP TEMPORARY TABLE
            # ------------------------------------------------

            drop_table(external_table_id)

            # ------------------------------------------------
            # UPDATE TOTALS
            # ------------------------------------------------

            total_expected_rows += expected_rows_this_batch

            total_loaded_rows += loaded_rows_this_batch

            processed_files += len(batch_uris)

            # ------------------------------------------------
            # BATCH AUDIT - SUCCESS
            # ------------------------------------------------

            write_audit({
                "batch_id": batch_id,
                "load_type": load_type,
                "asset_type": asset_type,
                "gcs_path": prefix,

                "expected_file_count": len(batch_uris),
                "processed_file_count": len(batch_uris),

                "expected_row_count": expected_rows_this_batch,
                "loaded_row_count": loaded_rows_this_batch,

                "status": "SUCCESS",

                "started_at": batch_started_at.isoformat(),
                "completed_at": utc_now().isoformat(),

                "error_message": None,
            })

            logging.info(
                "Completed %s: files=%s expected_rows=%s "
                "affected_rows=%s",
                batch_id,
                len(batch_uris),
                expected_rows_this_batch,
                loaded_rows_this_batch
            )

            # ------------------------------------------------
            # ROW COUNT WARNING
            # ------------------------------------------------

            if loaded_rows_this_batch != expected_rows_this_batch:

                logging.warning(
                    "Row count mismatch in %s: expected %s, "
                    "affected %s "
                    "(this can be normal with MERGE if some "
                    "rows were updates instead of inserts)",
                    batch_id,
                    expected_rows_this_batch,
                    loaded_rows_this_batch,
                )

        except Exception as exc:

            logging.exception(
                "Batch %s failed",
                batch_id
            )

            # ------------------------------------------------
            # CLEANUP TEMPORARY TABLE
            # ------------------------------------------------

            try:

                drop_table(external_table_id)

            except Exception:

                logging.exception(
                    "Could not drop temporary table %s",
                    external_table_id
                )

            # ------------------------------------------------
            # BATCH AUDIT - FAILED
            # ------------------------------------------------

            write_audit({
                "batch_id": batch_id,
                "load_type": load_type,
                "asset_type": asset_type,
                "gcs_path": prefix,

                "expected_file_count": len(batch_uris),
                "processed_file_count": 0,

                "expected_row_count": expected_rows_this_batch,
                "loaded_row_count": loaded_rows_this_batch,

                "status": "FAILED",

                "started_at": batch_started_at.isoformat(),
                "completed_at": utc_now().isoformat(),

                "error_message": str(exc)[:5000],
            })

            # Stop processing because this batch failed
            raise

    # ========================================================
    # OVERALL LOGGING
    # ========================================================

    logging.info(
        "Finished %s: %s files, %s total rows affected "
        "across %s batches",
        run_id,
        processed_files,
        total_loaded_rows,
        batch_number
    )


def process_metadata(load_type: str):
    """
    Loads symbols_valid_meta.csv into bronze_symbol_metadata,
    if that file exists under this load_type folder.

    Metadata gets one audit row because it is a single file.
    """

    metadata_path = (
        f"{load_type.lower()}/{METADATA_FILENAME}"
    )

    if not file_exists(metadata_path):

        logging.info(
            "No metadata file found at %s, skipping metadata load",
            metadata_path
        )

        return

    started_at = utc_now()

    batch_run_id = (
        f"{load_type.lower()}_metadata_"
        f"{started_at.strftime('%Y%m%d%H%M%S')}"
    )

    gcs_uri = (
        f"gs://{BUCKET_NAME}/{metadata_path}"
    )

    try:

        rows_affected = merge_metadata_file(
            gcs_uri,
            METADATA_TABLE
        )

        # ----------------------------------------------------
        # METADATA AUDIT - SUCCESS
        # ----------------------------------------------------

        write_audit({
            "batch_id": batch_run_id,
            "load_type": load_type,
            "asset_type": "metadata",
            "gcs_path": metadata_path,

            "expected_file_count": 1,
            "processed_file_count": 1,

            "expected_row_count": None,
            "loaded_row_count": rows_affected,

            "status": "SUCCESS",

            "started_at": started_at.isoformat(),
            "completed_at": utc_now().isoformat(),

            "error_message": None,
        })

        logging.info(
            "Finished metadata load: %s rows affected",
            rows_affected
        )

    except Exception as exc:

        logging.exception(
            "Metadata load failed"
        )

        # ----------------------------------------------------
        # METADATA AUDIT - FAILED
        # ----------------------------------------------------

        write_audit({
            "batch_id": batch_run_id,
            "load_type": load_type,
            "asset_type": "metadata",
            "gcs_path": metadata_path,

            "expected_file_count": 1,
            "processed_file_count": 0,

            "expected_row_count": None,
            "loaded_row_count": 0,

            "status": "FAILED",

            "started_at": started_at.isoformat(),
            "completed_at": utc_now().isoformat(),

            "error_message": str(exc)[:5000],
        })

        raise


@functions_framework.cloud_event
def gcs_to_bronze(cloud_event):
    """
    Entry point GCP calls on every GCS upload.

    Ignores everything except a file named exactly "_READY"
    landing directly under historical/ or incremental/.

    Example:
        historical/_READY
        incremental/_READY
    """

    event = cloud_event.data

    object_name = event["name"]

    logging.info(
        "GCS event received for: %s",
        object_name
    )

    parts = object_name.split("/")

    # --------------------------------------------------------
    # ONLY REACT TO:
    #
    # historical/_READY
    # incremental/_READY
    # --------------------------------------------------------

    if (
        len(parts) != 2
        or parts[1] != READY_MARKER_FILENAME
    ):

        logging.info(
            "Ignoring non-trigger file: %s",
            object_name
        )

        return

    load_type = parts[0].upper()

    if load_type not in {
        "HISTORICAL",
        "INCREMENTAL"
    }:

        logging.error(
            "Invalid load type in path: %s",
            load_type
        )

        return
    silver_run_id = (
        f"{load_type.lower()}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
)

    logging.info(
        "READY marker detected for load_type=%s "
        "- starting full run",
        load_type
    )

    # ========================================================
    # 1. METADATA
    # ========================================================

    process_metadata(load_type)

    # ========================================================
    # 2. STOCKS + ETFs
    # ========================================================

    for asset_type in ASSET_TYPES:

        target_table = (
            STOCK_TABLE
            if asset_type == "stocks"
            else ETF_TABLE
        )

        batch_size = (
            STOCK_BATCH_SIZE
            if asset_type == "stocks"
            else ETF_BATCH_SIZE
        )

        process_asset_type(
            load_type,
            asset_type,
            target_table,
            batch_size
        )

    logging.info(
        "All asset types processed for load_type=%s",
        load_type
    )

    write_silver_ready_marker(silver_run_id)
