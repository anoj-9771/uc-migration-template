# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

template = {
    "run_as": {
        "service_principal_name": ""
    },
    "name": "rbac-assignment",
    "email_notifications": {
        "no_alert_for_skipped_runs": False
    },
    "webhook_notifications": {},
    "timeout_seconds": 3600,
    "schedule": {
        "quartz_cron_expression": "0 00 8 ? * MON-FRI *",
        "timezone_id": "Australia/Sydney",
        "pause_status": "PAUSED"
    },
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "rbac-assign",
            "notebook_task": {
                "notebook_path": "/MDP-Framework/RBAC/rbac-job",
                "source": "WORKSPACE"
            },
            "job_cluster_key": "job-cluster",
            "timeout_seconds": 0,
            "email_notifications": {}
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "job-cluster",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "azure_attributes": {},
                "autoscale": {
                    "min_workers": 2,
                    "max_workers": 8
                },
                "node_type_id": "Standard_DS3_v2",
                "custom_tags": {
                    "product": "EDP Maintenance"
                },
                "data_security_mode": "USER_ISOLATION",
                "runtime_engine": "STANDARD",
                "num_workers": 1
            }
        }
    ],
    "format": "MULTI_TASK"
}
#print(CreateOrEditJob(template))

# COMMAND ----------


