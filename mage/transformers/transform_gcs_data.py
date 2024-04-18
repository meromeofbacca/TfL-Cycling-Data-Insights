import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data.drop(['Date', 'Time'], axis=1, inplace=True)
    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower()
    )
    data = data.rename(columns={'year':'counting_period'})
    print(data.dtypes)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
