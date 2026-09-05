from google.cloud import bigquery

PROJECT_ID = "market-lens-506611"
REGION = "us-central1"

client = bigquery.Client(project=PROJECT_ID)

print("Creating Gold views...")

with open("view.sql", "r", encoding="utf-8") as file:
    sql = file.read()

try:
    job = client.query(
        sql,
        location=REGION
    )

    job.result()

    print("Gold views created successfully.")

except Exception as e:
    print("Gold views creation failed.")
    print(f"Error: {e}")
    raise