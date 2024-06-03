
## Analytics Engineering with DBT

### Basic Commands
```bash
# Initialize the dbt project
dbt init
cd <dbt_project_path>

# Copy `profiles.yml` to the dbt project
cp <profiles_path> <dbt_project_path>  # eg. cp "C:\Users\WitchakornWanasanwon\.dbt\profiles.yml" .

# Validate configuration
dbt debug

# Create or Update tables in your target database
dbt run

# Test data quality
dbt test

dbt docs generate --models <model_name>

dbt docs serve

```

### Set the frequency cap
The minimum refresh frequency cap is 1 minute. The maximum refresh frequency cap is 7 days.

```yaml
{{
config(
    materialized = 'materialized_view',
    on_configuration_change = 'apply',
    enable_refresh = True,
    refresh_interval_minutes = 30
    max_staleness = 'INTERVAL 60 MINUTE'
)
}}
```

ref: https://cloud.google.com/bigquery/docs/materialized-views-manage