import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

#area global variables to input: central, inner, outer, cycleways

# Load data from all CSV URLs in chunks
urls = [
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q2%20spring%20(Apr-Jun)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q2%20spring%20(Apr-Jun)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q2%20spring%20(Apr-Jun)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q2%20spring%20(Apr-Jun)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q2%20spring%20(Apr-Jun)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q2%20spring%20(Apr-Jun)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q2%20spring%20(Apr-Jun)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q2%20spring%20(Apr-Jun)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q2%20spring%20(Apr-Jun)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2017%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q2%20spring%20(Apr-Jun)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q2%20spring%20(Apr-Jun)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q2%20spring%20(Apr-Jun)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2018%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q2%20spring%20(Apr-Jun)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q2%20spring%20(Apr-Jun)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q2%20spring%20(Apr-Jun)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2019%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q2%20spring%20(synthetic)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q2%20spring%20(synthetic)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q2%20spring%20(synthetic)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q4%20autumn%20(Oct-Dec)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2020%20Q4%20autumn%20(Oct-Dec)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q2%20spring%20(Apr-Jun)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q2%20spring%20(Apr-Jun)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q2%20spring%20(Apr-Jun)-Inner.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q2%20spring%20(Apr-Jun)-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q3%20(Jul-Sep)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q4%20autumn%20(Oct-Dec)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2021%20Q4%20autumn%20(Oct-Dec)-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20Q1%20(Jan-Mar)-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20W1%20spring-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20W1%20spring-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20W1%20spring-Inner-Part1.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20W1%20spring-Inner-Part2.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20W1%20spring-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2022%20W2%20autumn-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Central.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Cycleways.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Inner-Part1.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Inner-Part2.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W1%20spring-Outer.csv",
    "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2023%20W2%20autumn-Cycleways.csv"
]

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load data from API given a list of URLs in chunks
    """
    area = kwargs['area'].capitalize()
    print(area)
    area_urls = [url for url in urls if area in url]

    all_data_chunks = []
    for url in area_urls:
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
