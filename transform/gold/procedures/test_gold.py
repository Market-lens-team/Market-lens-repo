from google.cloud import bigquery

PROJECT_ID = "market-lens-506611"
REGION = "us-central1"

client = bigquery.Client(project=PROJECT_ID)

query = """
CALL `market-lens-506611.gold.sp_refresh_all`();
"""

print("Executing Gold stored procedure...")

try:
    job = client.query(
        query,
        location=REGION
    )

    job.result()

    print("Gold stored procedure executed successfully.")

except Exception as e:
    print("Gold stored procedure failed.")
    print(f"Error: {e}")
    raise