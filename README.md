# BigQuery Public Datasets Metadata

## BigQuery Public Datasets Metadata table
table is public and it can be accessed under

project: adventures-on-gcp  
dataset: bigquery_public_datasets  
table: bq_public_metadata  


this repository contains script which gets metadata from BigQuery public
repositories and stores them into separate BigQuery table.

It can be also deployed as Google Cloud Function and hooked up through Pub/Sub
and initiated from Cloud Scheduler.

## Setup
If you want to deploy this on your own there are some variables which need to be configured for file `settings.py`
based on `settings.py.example`

## Deployment of Cloud Function
using Google Cloud SDK:

`gcloud functions deploy bq_public_metadata  --runtime python37
--trigger-topic <pub-sub-topic> --timeout 540`

where `<pub-sub-topic>` is name of Pub/Sub topic which triggers Cloud Function and should be created beforehand

## Additional information
More about this project is written on [https://www.the-swamp.info/blog/bigquery-public-datasets-metadata/](https://www.the-swamp.info/blog/bigquery-public-datasets-metadata/)

GCP projects with public data in BigQuery are listed in file `projects_list.txt`. Feel free to crete pull request or 
write me message in case there is some public dataset which is not listed.
 