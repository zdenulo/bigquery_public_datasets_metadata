from bq_metadata import run_pipeline


def bq_public_metadata(event, contex):
    """Just wrapper for Google Cloud Function which is triggered by Pub/Sub"""

    run_pipeline()
