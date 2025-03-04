from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from config.client_config import ClientConfig
from utils.helpers import run_dbt_command

# Load client configuration
config = ClientConfig()

# Configuration using client info
BASE_DIR = Path(__file__).parent.parent
DBT_DIR = BASE_DIR / "dbt"  # Point to the dbt subdirectory


@dag(
    dag_id=config.project_name,
    description=f"dbt DAG for {config.client_display_name}",
    schedule_interval=config.schedule_interval,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pulse", config.client_display_name, config.client_id],
    default_args={"owner": config.client_display_name},
)
def dbt_dag():
    """dbt DAG for client"""

    @task
    def dbt_run():
        """Run dbt models"""
        context = get_current_context()
        dag_run_conf = context["dag_run"].conf or {}

        # Get task-specific config, defaulting to empty dict
        task_config = dag_run_conf.get("dbt_run", {})

        success = run_dbt_command(
            command="run",
            project_dir=DBT_DIR,
            profiles_dir=DBT_DIR,
            target=config.environment,
            **task_config,  # Pass all task-specific configs
        )

        if not success:
            raise Exception("dbt run failed")
        return "dbt run completed successfully"

    @task
    def dbt_test():
        """Test dbt models"""
        context = get_current_context()
        dag_run_conf = context["dag_run"].conf or {}

        # Get task-specific config, defaulting to empty dict
        task_config = dag_run_conf.get("dbt_test", {})

        success = run_dbt_command(
            command="test",
            project_dir=DBT_DIR,
            profiles_dir=DBT_DIR,
            target=config.environment,
            **task_config,  # Pass all task-specific configs
        )

        if not success:
            raise Exception("dbt test failed")
        return "dbt test completed successfully"

    # Define dependencies
    dbt_run() >> dbt_test()


# Instantiate DAG
dag_instance = dbt_dag()
