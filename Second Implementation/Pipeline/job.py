# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Final_Project_Job = Job.from_dict(
    {
        "name": "Final Project Job",
        "tasks": [
            {
                "task_key": "raw_to_bronze",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/kanade.pr@northeastern.edu/DAMG 7370/Final Project/Pipeline Notebooks/raw_to_bronze",
                    "base_parameters": {
                        "base_path": "/Volumes/workspace/final_project_bronze/food_inspection",
                        "catalog_name": "workspace",
                        "raw_schema": "final_project_raw",
                        "raw_table": "food_inspection",
                        "bronze_schema": "final_project_bronze",
                        "bronze_table": "food_inspection",
                    },
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "chicago_bronze_to_silver",
                "depends_on": [
                    {
                        "task_key": "raw_to_bronze",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/kanade.pr@northeastern.edu/DAMG 7370/Final Project/Pipeline Notebooks/chicago_bronze_to_silver",
                    "base_parameters": {
                        "catalog_name": "workspace",
                        "bronze_schema": "final_project_bronze",
                        "silver_schema": "final_project_silver",
                    },
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "dallas_bronze_to_silver",
                "depends_on": [
                    {
                        "task_key": "chicago_bronze_to_silver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/kanade.pr@northeastern.edu/DAMG 7370/Final Project/Pipeline Notebooks/dallas_bronze_to_silver",
                    "base_parameters": {
                        "catalog_name": "workspace",
                        "bronze_schema": "final_project_bronze",
                        "silver_schema": "final_project_silver",
                    },
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "silver_to_gold",
                "depends_on": [
                    {
                        "task_key": "dallas_bronze_to_silver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/kanade.pr@northeastern.edu/DAMG 7370/Final Project/Pipeline Notebooks/silver_to_gold",
                    "base_parameters": {
                        "catalog_name": "workspace",
                        "silver_schema": "final_project_silver",
                        "gold_schema": "final_project_gold",
                    },
                    "source": "WORKSPACE",
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Final_Project_Job, job_id=398760755426683)
# or create a new job using: w.jobs.create(**Final_Project_Job.as_shallow_dict())
