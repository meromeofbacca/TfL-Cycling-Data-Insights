import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    area = kwargs['area']
    data['Datetime'] = pd.to_datetime(data['Date'] + ' ' + data['Time'], format='%d/%m/%Y %H:%M:%S')
    data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y')
    if 'SiteID' in data.dtypes.to_list():
        data['UnqID'].fillna(data['SiteID'], inplace=True)
        data.drop('SiteID', axis=1, inplace=True)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
