import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data['Datetime'] = pd.to_datetime(data['Date'] + ' ' + data['Time'], format='%d/%m/%Y %H:%M:%S')
    data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y')
    data['Area'] = kwargs['area']
    df['UnqID'].fillna(df['SiteID'], inplace=True)
    df.drop('SiteId', axis=1, inplace=True)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
