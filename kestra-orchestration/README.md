## Extract and Load data into GCP/local via kestra
Extract csv files and load into GCP bucket with defined schema.

### Orchestration
- Config gcp credential and verify connections
- Call `flows/08_gcp_taxi.yaml` for each loop of multi-selected inputs


