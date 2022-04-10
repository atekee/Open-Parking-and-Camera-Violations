## Description
In this project, I extracted millions of rows from the NYC Open Parking and Camera Violations dataset, and loaded all the data into an Elasticsearch (OpenSearch) instance hosted on AWS and then analyzed it with creating a Dashboard on Kibana (OpenSearch Dashboard Platform).

## Dataset - API Reference
As of April 2022, NYC Open Parking and Camera Violations dataset has 78.6 million rows and 19 columns. Each row is an open parking and camera violations issued in NYC traced backed from 2000 to now. 

https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89

## Tech Used
EC2, the terminal, Docker, ElasticSearch, Kibana, Python, Socrata API

## Folder Structure
project01/

+-- Dockerfile

+-- requirements.txt

+-- src/

+-- +-- main.py

+-- assets/

+-- +-- kibana_dashboard.png

+-- README

## Usage

Step 1: Provision an EC2 instance and ElasticSearch cluster on AWS

Step 2: Build the docker image
```
docker build -t bigdataproject1:1.0 .
````

Step 3: Run the docker container
```
docker run \
-v $PWD:/app \
-e DATASET_ID="XXX" \
-e APP_TOKEN="XXX" \
-e ES_HOST="XXX" \
-e INDEX_NAME="XXX" \
-e ES_USERNAME="XXX" \
-e ES_PASSWORD="XXX" \
bigdataproject1:1.0 --page_size=100 --num_pages=10
````

Step 4: Logging in to the opensearchservice domain, the uploaded data can be visualized. 


## Observations
