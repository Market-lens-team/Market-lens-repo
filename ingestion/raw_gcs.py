"""
This script uploads your historical data (stocks/ and etfs/ folders) to a
GCS bucket - in PARALLEL, using multiple threads at once, instead of
uploading one file after another.

Before running this, make sure:
  1. You have a GCP project and a GCS bucket already created.
  2. You have authenticated locally, either by running:
         gcloud auth application-default login
     or by setting an environment variable pointing to a service account key:
         GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
  3. You have installed the required library:
         pip install google-cloud-storage
"""

# import os lets us walk through folders and build file paths
import os

# ThreadPoolExecutor lets us run many upload tasks at the same time,
# instead of waiting for each one to finish before starting the next.
# as_completed lets us process results as each upload finishes (in any order).
from concurrent.futures import ThreadPoolExecutor, as_completed

# import the Google Cloud Storage client library
# this is what actually talks to GCS on your behalf
from google.cloud import storage


# ---- CONFIG ----
# the name of your GCS bucket (must already exist in your GCP project)
BUCKET_NAME = "market-lens-506611-raw-mlteam-2026"

# the root folder on your machine where stocks/ and etfs/ live
# the "r" before the string makes it a raw string, so backslashes in the
# Windows path are treated literally instead of as escape characters
BASE_DIR = r"E:\Downloads\MarketLens_Dataset"

# the local folders you want to upload (historical data only)
# these are just the sub-folder names - BASE_DIR is joined with these below
LOCAL_FOLDERS = ["stocks", "etfs"]

# the "folder path" prefix you want these files to live under, inside the bucket
GCS_DESTINATION_PREFIX = "historical"

# how many uploads to run at the same time
# uploading is mostly "waiting on the network", so a higher number than your
# CPU core count is fine here - 16-32 is a reasonable starting point
MAX_WORKERS = 16


def upload_one_file(bucket, local_file_path: str, gcs_destination_path: str):
    """
    Uploads a single file to GCS. This function is what each parallel
    "worker" thread will run, once per file.
    """

    # a "blob" in GCS terms is just a single file/object inside the bucket
    # this creates a reference to where the file WILL live, before uploading
    blob = bucket.blob(gcs_destination_path)

    # this line actually uploads the local file's contents to that destination
    blob.upload_from_filename(local_file_path)

    # return a status string so we can print progress once this thread finishes
    return f"Uploaded: {local_file_path}  -->  gs://{bucket.name}/{gcs_destination_path}"


def build_upload_tasks(local_folder: str, gcs_prefix: str):
    """
    Instead of uploading immediately, this just builds a list of
    (local_path, destination_path) pairs for every file in a folder.
    We collect ALL tasks first (across both stocks/ and etfs/), then
    hand them all to the thread pool together.
    """

    tasks = []

    # join BASE_DIR with the sub-folder name to get the full real path on disk,
    # e.g. "E:\Downloads\MarketLens_Dataset\stocks"
    full_folder_path = os.path.join(BASE_DIR, local_folder)

    # os.listdir() gives us the list of filenames sitting inside this folder
    filenames = os.listdir(full_folder_path)

    for filename in filenames:

        # build the full local file path using the full folder path,
        # e.g. "E:\Downloads\MarketLens_Dataset\stocks\AAPL.csv"
        local_file_path = os.path.join(full_folder_path, filename)

        # skip anything that isn't a file (just in case there's a stray sub-folder)
        if not os.path.isfile(local_file_path):
            continue

        # build the destination path inside the bucket, e.g. "raw/stocks/AAPL.csv"
        # note: this still uses just "local_folder" (not the full path) so the
        # bucket structure stays clean, e.g. raw/stocks/... not raw/E:/Downloads/.../stocks/...
        gcs_destination_path = f"{gcs_prefix}/{local_folder}/{filename}"

        # add this (source, destination) pair to our task list
        tasks.append((local_file_path, gcs_destination_path))

    return tasks


def main():
    # create a client object - this is your authenticated connection to GCS
    client = storage.Client()

    # get a reference to your specific bucket (does not create it - it must already exist)
    bucket = client.bucket(BUCKET_NAME)

    # this will hold every (local_path, destination_path) pair across all folders
    all_tasks = []

    # build the full task list first, before uploading anything
    for folder in LOCAL_FOLDERS:

        # build the full path for this folder so we can check it actually exists
        full_folder_path = os.path.join(BASE_DIR, folder)

        # safety check - skip if the folder doesn't exist locally
        if not os.path.isdir(full_folder_path):
            print(f"Skipping missing folder: {full_folder_path}")
            continue

        folder_tasks = build_upload_tasks(folder, GCS_DESTINATION_PREFIX)
        all_tasks.extend(folder_tasks)

    print(f"Total files to upload: {len(all_tasks)}")
    print(f"Uploading with {MAX_WORKERS} parallel workers...\n")

    # ThreadPoolExecutor creates a pool of worker threads that run tasks concurrently
    # "with" makes sure the pool is properly shut down once we're done
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        # submit() schedules one upload task per file - this starts them running
        # in the background immediately, without waiting for each to finish.
        # we build a dictionary mapping each "future" (a pending result) back to
        # its file path, so if something fails we know which file it was.
        future_to_path = {
            executor.submit(upload_one_file, bucket, local_path, dest_path): local_path
            for local_path, dest_path in all_tasks
        }

        # as_completed() gives us each future AS SOON AS it finishes,
        # regardless of the order they were submitted in
        for future in as_completed(future_to_path):

            local_path = future_to_path[future]

            try:
                # .result() gets the return value from upload_one_file()
                # if the upload raised an exception, .result() will re-raise it here
                result_message = future.result()
                print(result_message)

            except Exception as e:
                # catch and report failures per-file, without stopping the
                # rest of the uploads that are still running
                print(f"FAILED to upload {local_path}: {e}")

    # final confirmation message once everything is done
    print("\nAll historical data uploaded to GCS.")


# this makes sure main() only runs when you execute this file directly,
# not if it gets imported into another script
if __name__ == "__main__":
    main()