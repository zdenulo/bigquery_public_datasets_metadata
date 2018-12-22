# BigQuery Public Datasets Metadata

this repository contains script which gets metadata from BigQuery public
repositories and stores it into separate BigQuery table.

It can be also deployed as Google Cloud Function and hooked up through Pub/Sub
and initiated from Cloud Scheduler.

## Setup
There are some variables which need to be configured for file `settings.py`
based on `settings.py.example`

## Deployment of Cloud Function
using Google Cloud SDK:

`gcloud functions deploy bq_public_metadata  --runtime python37
--trigger-topic <pub-sub-topic> --timeout 540`

where `<pub-sub-topic>` is name of Pub/Sub topic which triggers Cloud Function.
