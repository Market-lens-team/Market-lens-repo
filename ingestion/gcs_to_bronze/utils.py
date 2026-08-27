import csv
import io
import logging
from pathlib import Path

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage

from config import (
    PROJECT_ID,
    REGION,
    BUCKET_NAME,
    BRONZE_DATASET,
    QUARANTINE_PREFIX,
)


logging.basicConfig(level=logging.INFO)


storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)


PRICE_HEADERS = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Adj Close",
    "Volume",
]


def list_csv_files(prefix: str):

    blobs = storage_client.list_blobs(
        BUCKET_NAME,
        prefix=prefix,
    )

    return [
        blob
        for blob in blobs
        if blob.name.lower().endswith(".csv")
        and blob.size
        and blob.size > 0
    ]


def read_header(blob):

    raw = blob.download_as_bytes(
        start=0,
        end=8191,
    )

    text = raw.decode(
        "utf-8-sig",
        errors="replace",
    )

    lines = text.splitlines()

    if not lines:
        return []

    return next(
        csv.reader(
            io.StringIO(lines[0])
        )
    )


def validate_price_file(blob):

    try:

        actual = [
            value.strip()
            for value in read_header(blob)
        ]

        if actual != PRICE_HEADERS:
            return False, f"HEADER_MISMATCH: {actual}"

        return True, None

    except Exception as exc:

        return False, f"HEADER_READ_ERROR: {exc}"



def quarantine_file(blob, asset_type, reason):

    bucket = storage_client.bucket(
        BUCKET_NAME
    )

    destination = (
        f"{QUARANTINE_PREFIX}/"
        f"{asset_type}/"
        f"{Path(blob.name).name}"
    )

    bucket.copy_blob(
        blob,
        bucket,
        destination,
    )

    logging.error(
        "Quarantined %s because %s",
        blob.name,
        reason,
    )



def create_external_table(
    external_table_name,
    uris,
):

    table_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"{external_table_name}"
    )


    source_schema = [

        bigquery.SchemaField(
            "Date",
            "STRING"
        ),

        bigquery.SchemaField(
            "Open",
            "STRING"
        ),

        bigquery.SchemaField(
            "High",
            "STRING"
        ),

        bigquery.SchemaField(
            "Low",
            "STRING"
        ),

        bigquery.SchemaField(
            "Close",
            "STRING"
        ),

        bigquery.SchemaField(
            "Adj Close",
            "STRING"
        ),

        bigquery.SchemaField(
            "Volume",
            "STRING"
        ),
    ]

    external_config = bigquery.ExternalConfig(
        bigquery.ExternalSourceFormat.CSV
    )


    external_config.source_uris = uris

    external_config.options.skip_leading_rows = 1

    external_config.schema = source_schema


    table = bigquery.Table(
        table_id,
        schema=source_schema,
    )


    table.external_data_configuration = (
        external_config
    )


    create_retry = retry.Retry(
        initial=2.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=120,
    )


    create_retry(
        bq_client.create_table
    )(
        table,
        exists_ok=True,
    )


    return table_id



def delete_external_table(table_id):

    bq_client.delete_table(
        table_id,
        not_found_ok=True,
    )



def load_price_batch(
    uris,
    target_table,
    external_table_name,
):


    external_table = create_external_table(
        external_table_name,
        uris,
    )


    target_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"{target_table}"
    )


    query = f"""

    INSERT INTO `{target_id}`
    (
        Date,
        symbol,
        Open,
        High,
        Low,
        Close,
        Adj_Close,
        Volume
    )


    SELECT

        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', Date),
            SAFE.PARSE_DATE('%d-%m-%Y', Date),
            SAFE.PARSE_DATE('%m/%d/%Y', Date)
        ) AS Date,


        REGEXP_EXTRACT(
            _FILE_NAME,
            r'/([^/]+)\\.csv$'
        ) AS symbol,


        SAFE_CAST(Open AS FLOAT64),

        SAFE_CAST(High AS FLOAT64),

        SAFE_CAST(Low AS FLOAT64),

        SAFE_CAST(Close AS FLOAT64),


        SAFE_CAST(
            `Adj Close`
            AS FLOAT64
        ),


        SAFE_CAST(Volume AS FLOAT64) AS Volume

    FROM `{external_table}`

    """


    try:

        logging.info(
            "Loading batch into %s",
            target_table,
        )
        logging.info(
            "Executing price batch query:\n%s",
            query,
        )


        job = bq_client.query(
            query,
            location=REGION,
        )


        job.result()


        logging.info(
            "Batch loaded successfully"
        )


        return (
            job.num_dml_affected_rows
            or 0
        )


    finally:

        delete_external_table(
            external_table
        )



def write_audit(record):

    table_id = (
        f"{PROJECT_ID}."
        f"{BRONZE_DATASET}."
        f"ingestion_audit"
    )


    try:

        insert_retry = retry.Retry(
            initial=2.0,
            maximum=30.0,
            multiplier=2.0,
            deadline=120,
        )


        insert_rows = insert_retry(
            bq_client.insert_rows_json
        )


        errors = insert_rows(
            table_id,
            [record],
        )


        if errors:

            logging.error(
                "Audit insert failed: %s",
                errors,
            )

        else:

            logging.info(
                "Audit record inserted successfully"
            )


    except Exception as exc:

        logging.exception(
            "Audit write failed: %s",
            exc
        )