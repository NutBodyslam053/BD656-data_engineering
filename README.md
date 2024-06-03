# BD656: Reddit API Data Pipeline Project

```yaml
Author: Witchakorn Wanasanwongkot
Student ID: 65130459
```

## Data Ingestion Pipeline

![](images/diagram-data_pipeline.png)


## Data Pipeline Orchestration

### Airflow

![](images/airflow-pipeline.png)

## Raw Data

**Source**: API reddit r/dataengineer

![](images/api-raw_data.png)

## Transformed Data

### Google Cloud Storage (raw bucket): JSON format
![](images/gcs-raw_bucket.png)

### Google Cloud Storage (processed bucket): PARQUET format
![](images/gcs-processed_bucket.png)

### BigQuery: Table/View
![](images/bq-table.png)

## Visualization

### Metabase
![](images/metabase-dashboard.png)


## Setting Up

Clone the repository to local
```bash
git clone https://github.com/NutBodyslam053/BD656-data_engineering.git
cd BD656-data_engineering
```

```bash
docker compose up --build -d
```

Access to Airflow
```bash
localhost:8080
```

- Add variables to Airflow environment
- Add connection to GCP

Access to Metabase
```bash
localhost:3000
```

- Add connection to BigQuery