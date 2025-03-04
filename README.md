# Pulse Client Template

This repository serves as a template for Pulse client dbt projects that run in Airflow. It provides a standardized structure for data transformation pipelines that publish data to dashboards via Google Cloud Platform services.

## Project Overview

The Pulse Client Template is designed to:
- Execute dbt models in an Airflow environment using TaskFlow API
- Upload dbt artifacts to Google Cloud Storage
- Publish successful table information to Pub/Sub for dashboard consumption
- [TODO] Provide robust error handling and alerting

## Repository Structure
```
.
├── .github/                      # GitHub configuration files
├── .vscode/                      # VS Code settings
├── config/                       # Configuration management
│   ├── __init__.py
│   └── client_config.py          # Client configuration class
├── dags/                         # Airflow DAG definitions
│   └── dag.py                    # Main TaskFlow DAG
├── dbt/                          # DBT project files
│   ├── models/                   # dbt models directory
│   ├── dbt_project.yml           # dbt project configuration
│   ├── packages.yml              # dbt package dependencies
│   └── profiles.yml              # dbt connection profiles
├── templates/                    # Message templates
│   └── slack/                    # Slack message blocks
├── utils/                        # Utility modules
│   ├── __init__.py
│   ├── alerts.py                 # Alert handling for failures
│   └── helpers.py                # Helper functions for dbt execution
├── dag_config.json               # Client-specific DAG configuration
└── keyfile.json                  # GCP service account credentials (template)
```

## Key Components

### Configuration Management
- **client_config.py**: Centralized configuration class that:
  - Loads settings from dag_config.json
  - Handles environment variables
  - Provides consistent access to client properties

### DAG Configuration
The Airflow DAG (`dags/dag.py`) uses TaskFlow API to orchestrate:
1. **dbt Run Task**: Executes dbt models with configurable parameters
2. **dbt Test Task**: Runs dbt tests with configurable parameters
3. **Publish Tables Task**: Publishes successful tables to Pub/Sub

### Utility Functions
- **helpers.py**: Contains functions for:
  - Running dbt commands with proper environment setup
  - Uploading artifacts to Google Cloud Storage
  - Querying BigQuery for table information
  - Publishing messages to Pub/Sub

### Environment Configuration
- Supports both `dev` and `prod` environments via config
- Uses BigQuery for data storage
- Relies on GCS for artifact storage
- Leverages Pub/Sub for messaging

## Workflow

1. The DAG is triggered (manually or on schedule)
2. dbt dependencies are automatically installed
3. dbt models are executed with the specified parameters
4. Results and artifacts are uploaded to GCS
5. Tests are executed against the models
6. Tables are published to Pub/Sub for dashboard consumption
7. Any failures trigger alerts

## Runtime Configuration

The DAG supports runtime configuration through Airflow's "Run DAG with Config" feature:

```json
{
  "dbt_run": {
    "select": "model1 model2+",
    "exclude": "deprecated_models",
    "full_refresh": true,
    "vars": {"currency": "EUR"}
  },
  "dbt_test": {
    "select": "+Presentation",
    "exclude": "test_name:relationships",
    "vars": {"strict": true}
  }
}
```
This allows for flexible execution without modifying the code.

## Usage

This template is intended to be customized for specific clients by:
1. Updating `dag_config.json` with client-specific information
2. Adding client-specific dbt models to the `models` directory
3. Configuring the appropriate environment variables
4. Providing a valid GCP service account with appropriate permissions

## Dependencies

- dbt Core (>=1.0.0 and <2.0.0)
- Apache Airflow
- Google Cloud libraries (bigquery, pubsub_v1, storage)
- Other Python dependencies (yaml, openpyxl, etc.)

## Future Improvements

This codebase is slated for revamping to:
- Simplify the structure
- Follow best practices more closely
- Improve efficiency in execution
- Enhance documentation and maintainability
