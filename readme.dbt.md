
## Analytics Engineering with DBT

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
```