import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/1%20Monitoring%20locations.csv'
    
    all_data_chunks = []
    response = requests.get(url)
    # Read CSV in chunks
    for chunk in pd.read_csv(io.StringIO(response.text), sep=',', chunksize=1000):
        all_data_chunks.append(chunk)
    return pd.concat(all_data_chunks, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
