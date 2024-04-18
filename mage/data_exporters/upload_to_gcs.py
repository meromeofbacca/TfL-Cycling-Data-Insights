import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp-key.json"

    area = kwargs['area']
    bucket_name = 'de-zoomcamp-420207-viet-bucket'
    project_id = 'de-zoomcamp-420207'
    table_name = f'uk_cycling_{area}'
    root_path = f'{bucket_name}/{table_name}'

    data['Date'] = data['Date'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['Date'],
        filesystem=gcs
    )

