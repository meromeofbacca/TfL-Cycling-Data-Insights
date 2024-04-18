# TfL Cycling

## Overview
The Active Travel Counts Programme by Transport for London (TfL) compiles historical and ongoing data on cycling, e-scooters, and pedestrian counts in London since 2014. Initially focused on monitoring cycling volumes, the programme expanded in 2022 to include counts of e-scooters and pedestrians, providing a comprehensive dataset for analysis and visualization.

The goal of this project is to build an end to end data pipeline to ingest, store, transform and visually analyze TfL cycling data. The pipeline will source its data from the TfL cycling data page (https://cycling.data.tfl.gov.uk/), ingest it using Mage and then export it to a data lake in Google Cloud Storage. The data will be transported to Google BigQuery where transformations and table joins will be performed in dbt and loaded back into BigQuery. Data visualization will then take place in a dashboard in Looker Studio.

## Technologies

- Containerization: **Docker**
- IaC: **Terraform**
- Orchestration: **Mage** 
- Data Lake: **Google Cloud Storage**
- Data Warehouse: **BigQuery**
- Transformation: **dbt**
- Data Visualization: **Looker Studio**

## Data Pipeline Architecture

![](images/Architecture.png)

## Data Visualization

View the Looker Studio dashboard [here](https://lookerstudio.google.com/reporting/91e140aa-d586-4d5b-a42f-43c68d5754b0).

![](images/TfL_Cycling_Analytics.jpg)

## Reproduce Project
