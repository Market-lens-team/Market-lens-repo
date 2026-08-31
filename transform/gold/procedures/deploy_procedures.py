import os
from google.cloud import bigquery

client = bigquery.Client(project="market-lens-506611")
procedures_dir = os.path.dirname(os.path.abspath(__file__))

for filename in sorted(os.listdir(procedures_dir)):
    if filename.endswith(".sql"):
        path = os.path.join(procedures_dir, filename)
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()

        print(f"Deploying {filename}...")
        job = client.query(sql, location="us-central1")
        job.result()  # waits for completion, raises if it fails
        print(f"Done: {filename}")

print("All procedures deployed.")