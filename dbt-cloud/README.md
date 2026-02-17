## Analytics Engineering in dbt cloud

### Staging models
- Stage Green, yellow and fhv trip data into staging models filtering out null IDs

### Intermediate
- Merge Green and Yellow trip data into one table
- Join categorical values such as payment_type
- Deduplicate records and add unique key

### Marts
- Create dimension tables for taxi zones and payment types from dbt seeds
- Build fact table from intermediate models
- Build revenue reporting tables for different zones on monthly basis