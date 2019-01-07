import json
import datetime
import logging
from io import StringIO

from google.cloud import bigquery
from google.cloud import storage

from settings import *


def get_bq_data():
    """Gets metadata from public BigQuery datasets"""

    with open('projects_list.txt') as f:
        projects = f.readlines()
    projects = [p.strip() for p in projects if p]

    data = []

    for project in projects:
        client = bigquery.Client(project=project)
        datasets = client.list_datasets()

        for dataset in datasets:
            dataset_id = dataset.dataset_id
            dataset_ref = client.get_dataset(dataset_id)
            tables = client.list_tables(dataset_id)
            for table in tables:
                full_table_id = table.full_table_id.replace(':', '.')
                table_ref = client.get_table(full_table_id)

                item = {'dataset_id': dataset_id,
                        'project_id': project,
                        'table_id': table_ref.table_id,
                        'dataset_description': dataset_ref.description,
                        'table_modified': table_ref.modified.strftime("%Y-%m-%d %H:%M:%S"),
                        'table_created': table_ref.created.strftime("%Y-%m-%d %H:%M:%S"),
                        'table_description': table_ref.description,
                        'table_num_bytes': table_ref.num_bytes,
                        'table_num_rows': table_ref.num_rows,
                        'table_partitioning_type': table_ref.partitioning_type,
                        'table_type': table_ref.table_type,
                        }
                data.append(item)
    return data


def upload_to_gcs(data):
    """Uploads list of data into Google Cloud Storage (GCS)
    Returns filename of created file
    """

    f = StringIO()
    for item in data:
        f.write(json.dumps(item) + '\n')

    logging.debug("uploading to GCS")
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket = gcs_client.bucket(GCS_BUCKET)
    bq_filename = 'bq_metadata_{}'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    blob = bucket.blob(bq_filename)
    blob.upload_from_file(f, rewind=True)
    f.close()
    return bq_filename


def upload_to_bq(bq_filename):
    """Uploads json file from GCS to BigQuery"""
    bq_client = bigquery.Client(project=PROJECT_ID)
    logging.debug("uploading to BQ")
    job_id = 'bq_import_{}'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    gcs_file_path = 'gs://' + GCS_BUCKET + '/' + bq_filename

    job_config = bigquery.job.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField('project_id', 'STRING', description='GCP project where from where public dataset is'),
        bigquery.SchemaField('dataset_id', 'STRING', description='BigQuery dataset'),
        bigquery.SchemaField('table_id', 'STRING', description='Table id (name) of public dataset'),
        bigquery.SchemaField('dataset_description', 'STRING'),
        bigquery.SchemaField('table_description', 'STRING', ),
        bigquery.SchemaField('table_created', 'TIMESTAMP', description='Datetime when table was created'),
        bigquery.SchemaField('table_modified', 'TIMESTAMP',
                             description='Datetime when table was last time modified (inserted rows)'),
        bigquery.SchemaField('table_num_bytes', 'INTEGER', description='Size of table in bytes'),
        bigquery.SchemaField('table_num_rows', 'INTEGER', description='Number of rows in the table'),
        bigquery.SchemaField('table_type', 'STRING',
                             description='Whether table is normal table, view or BQ ML model'),
        bigquery.SchemaField('table_partitioning_type', 'STRING',
                             description='Whether and how table is partitioned'),

    ]
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    dataset_ref = bq_client.dataset(OUT_DATASET_ID)
    table_ref = bigquery.TableReference(dataset_ref, OUT_TABLE_ID)
    job = bigquery.LoadJob(job_id, [gcs_file_path], client=bq_client, destination=table_ref, job_config=job_config)
    job.result()
    logging.debug("done")


def run_pipeline():
    data = get_bq_data()
    bq_filename = upload_to_gcs(data)
    upload_to_bq(bq_filename)
