import pandas as pd 
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.replace('(', '')
                    .str.replace(')', '')
                    .str.lower()
    )
    data = data.rename(columns={'is_it_on_the_strategic_cio_panel?': 'is_it_on_the_strategic_cio_panel'})
    print(data.columns.to_list())
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
