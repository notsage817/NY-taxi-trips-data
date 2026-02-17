##Data Warehouse

- Download csv data and parquet data from different source
- Upload files into Google Cloud Platform Bucket

Build docker image first

```docker build -t fhv-ingest . ```

Run docker with gcp credential in the codespace env

```docker run -e GCP_CREDENTIAL fhv-ingest```

(check credential name `echo $GCP_CREDENTIAL`)

Create extension table in Big Query

```
CREATE OR REPLACE EXTERNAL TABLE `your-project-id.ny_taxi.fhv_tripdata_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ny_taxi_bkt_de_dtc/fhv_tripdata_2019-*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1
);
```

Add ext table in the source.yml file into dbt-cloud/models/staging

