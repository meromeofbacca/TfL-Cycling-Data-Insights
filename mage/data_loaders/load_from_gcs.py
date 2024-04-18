import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import GcsFileSystem

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    area = kwargs['area']
    bucket_name = 'de-zoomcamp-420207-viet-bucket'
    blob_prefix = f'uk_cycling_{area}'
    root_path = f"{bucket_name}/{blob_prefix}"

    pa_table = pq.read_table(
        source=root_path,
        filesystem=GcsFileSystem(),
    )

    table = pa_table.to_pandas()
    print(table.dtypes)
    return table


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
