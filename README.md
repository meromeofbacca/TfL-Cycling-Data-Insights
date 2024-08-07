<h1 align="center">Transport for London Cycling</h1>
<p align="center">Data Engineering Project by Viet Dinh</p>

![](images/Cyclists_at_Hyde_Park.jpg)

## Overview
The Active Travel Counts Programme by Transport for London (TFL) compiles historical and ongoing data on cycling, e-scooters, and pedestrian counts in London since 2014. Initially focused on monitoring cycling volumes, the programme expanded in 2022 to include counts of e-scooters and pedestrians, providing a comprehensive dataset for analysis and visualization.

The goal of this project is to build an end to end data pipeline to ingest, store, transform and visually analyze Transport for London (TFL) cycling data. The pipeline will source its data from the TFL cycling data page (https://cycling.data.tfl.gov.uk/), ingest it using Mage and then export it to a data lake in Google Cloud Storage. The data will be transported to Google BigQuery where transformations and table joins will be performed in dbt and loaded back into BigQuery. Data visualization will then take place in a dashboard in Looker Studio.

## Objective/Problem
The data will be analyzed to observe cycling patterns in London over time, and generate insights on the effect of COVID on cyclings counts. Data on the most dense cycling areas in London will be revealed as well as the most popular months to cycle in London. This data can be used to help a cyclist decide when to take bike rides and where, and if they would rather avoid other cyclists or ride with them.
## Technologies

- Containerization: **Docker**
- IaC: **Terraform**
- Workflow Orchestration: **Mage** 
- Data Lake: **Google Cloud Storage**
- Data Warehouse: **BigQuery**
- Transformation: **dbt**
- Data Visualization: **Looker Studio**

## Data Pipeline Architecture

![](images/Architecture.jpg)

# Batch ingestion
Data is separated by area (Cycleways, Central, Inner, Outer) and is batch ingested from TFL source data using Mage. A datetime column is added by combining the date and time columns, and a deprecated SiteID field that has been replaced by the UnqID field has its data transferred to the UnqID column and subsequently dropped. The cycling data is then exported in GCS partitioned by day using Mage.

# Loading to Data Warehouse
Data is loaded from GCS by area and where slight transformations take place. Column names are normalized to lowercase, underscores are used to replace spaces, and columns are renamed. The data is also partitioned by date and clustered by unqid. Partitioning by datetime is useful as data from various time periods can be easily extracted, and clustering on unqid is important as often you would be grouping by the count location.

# Transformations
Using dbt, the four tables defined by area are given a surrogate key formed from the counting_period, datetime and unqid fields. The tables are unioned together and joined with a reference table of monitoring locations to create a fact table.

## Data Visualization

View the Looker Studio dashboard [here](https://lookerstudio.google.com/reporting/91e140aa-d586-4d5b-a42f-43c68d5754b0).

![](images/TfL_Cycling_Analytics.jpg)

# Reproduce Project

## Setting up virtual machine
- Create a Google Cloud Platform virtual machine and create GCP service accounts with permissions for GCS and BQ
- Clone this directory to your VM
- Install Anaconda, Google Cloud SDK, Terraform and Docker
    - Make sure to authenticate your google service account credentials on your VM
## Terraform and Docker
- Change arguments in Terraform files
    - In variables.tf change the project id, and region to your project id and region. 
    - Also add the location of your google credentials json file, and change names of buckets/datasets to your liking
    - Run terraform init, terraform plan and terraform apply to start your GCP buckets and BQ datasets
- Run docker-compose up to start a mage instance
## Orchestration and ingestion with Mage
- Make sure to forward port 6789 to connect to mage (localhost:6789)
- Once in mage create three pipelines:
    1) One to ingest TfL data and export it to a GCS bucket
       Files are
       - Data loader: load_tfl_api.py
       - Data transformer: transform_tfl.py
       - Data exporter: upload_to_gcs.py
    2) One to extract TfL data from a GCS bucket and load it to BigQuery
       Files are
       - Data loader: load_from_gcs.py
       - Data transformer: transform_gcs_data.py
       - Data exporter: gcs_to_bq.sql
    3) One to load a reference table monitoring_locations.csv into BigQuery
       Files are
       - Data loader: load_static_table_api.py
       - Data transformer: transform_static_data.py
       - Data exporter: static_table_to_gcs.sql
- Once the pipelines are created, create a runtime variable **area** in both pipelines 1 and 2.
  - The values of the runtime variable **area** will be cycleways, central, inner, outer.
![](runtime-variable.png)
1) Run pipeline 1 each time using a different value for the **area** runtime variable
2) Run pipeline 2 each time using a different value for the **area** runtime variable
3) Run pipeline 3
- Make sure the data exists in your GCS bucket and your BigQuery dataset

## Transformations with dbt

## Setup project
- Go to dbt cloud and create a new project
  - Make sure to connect the github repository to the one containing your project
  - Subdirectory should be dbt if no changes are made to the cloned repository
  - Connection should be to BigQuery
## Changing files
- Enter into the cloud cli
- Go to the dbt_project.yml and change the name under models: to be your BigQuery dataset name
- Go to the models/staging directory and inside schema.yml change the database id to your database id, and the schema to your BigQuery dataset
    - Make sure the table names match the ones in BigQuery
## Build models
- Run dbt build --vars '{'is_test_run':'false'}' (use 'true' to do a test run)
- Refresh BigQuery to find your models in the new dataset dbt created under the name {your_bigquery_dataset}_models

# Next Steps/Improvements
- Implement spark to create more exploratory tables using parallization
- Add more clustering fields like borough
- Use Tableau to create a concise and visually improved dashboard
- Further documentation on CI/CD dbt jobs/reproducing project
- Create a CI/CD pipeline beyond dbt CI checks
- Add tests to Mage pipeline + triggers
