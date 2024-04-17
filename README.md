# spotify-etl-pipeline-project

## Introduction
In this project, we will build an ETL (Extract, Transform, Load) pipeline using the spotify API on AWS. This pipeline extracts spotify information, transforms the data into a desired set of 3 data tables, then stores it within an AWS data store.

## About API/Datasets
This API contains information about the artists, albums, and songs - [Spotify API Docs](https://developer.spotify.com/documentation/web-api)

## Services Used
1. **Amazon S3 (Simple Storage Service):** This is a highly scalable storage service that can store and retrieve any data from the web. It is most commonly used to store and distribute large data files, data backups, and static website files.

2. **AWS Lambda:** This is a serverless compute service that allows you to develop and run functions without server management. These functions can be triggered by events that are linked to S3, Cloud Watch, DynamoDB, or other AWS services.

3. **Cloud Watch:** This is a monitoring service for AWS resources and the applications that are run on them. You can utilize Cloud Watch to set alarms, track metrics, and more. 

4. **Glue Crawler:** This is a managed service that automatically crawls and sifts through your data sources, identifies formats, and infers schemas to create an AWS data catalog. 

5. **Data Catalog:** This is a fully managed metadata repository that allows you to discover and manage data on S3 more seamlessly. These data catalogs can be used with other AWS servies such as AWS Athena. 

6. **AWS Athena:** This is an interactive query services designed to conduct analysis on your data in S3 via SQL. AWS Athena can let you run queries and store new found tables back in S3.


## Install Packages
```
pip install pandas
pip install numpy
pip install spotipy
```

## Project Architecture/Execution Flow
Extract data from API -> Lambda Trigger (Once a day) -> Run Extraction function -> Store raw data -> Trigger Transformation function -> Transform Data and load it -> Query transformed data using AWS Athena 
