# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

template = {
    "run_as": {
        "user_name": ""
    },
    "name": "daily-maintenance-script",
    "email_notifications": {},
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "schedule": {
        "quartz_cron_expression": "1 30 8 * * ?",
        "timezone_id": "Australia/Sydney",
        "pause_status": "UNPAUSED"
    },
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "InitialMaintenanceScript-curated",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "/MDP-Framework/Maintenance/InitialMaintenanceScript",
                "base_parameters": {
                    "P_logpath": "",
                    "P_TableNameLoc": "",
                    "P_TableName": "ALL TABLES",
                    "P_Schemanm": "",
                    "P_Schemapath": ""
                },
                "source": "WORKSPACE"
            },
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "13.3.x-scala2.12",
                "spark_conf": {
                },
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "ON_DEMAND_AZURE",
                    "spot_bid_max_price": -1
                },
                "autoscale": {
                        "min_workers": 4,
                        "max_workers": 8
                    }
                "node_type_id": "Standard_E4ds_v4",
                "custom_tags": {
                    "product": "EDP Maintenance"
                },
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "enable_elastic_disk": "true",
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD"
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": "false",
                "no_alert_for_canceled_runs": "false",
                "alert_on_last_attempt": "false"
            }
        }
    ],
    "format": "MULTI_TASK"
}

template["schedule"]["pause_status"] = "UNPAUSED" if GetEnvironmentTag() == "PROD" else "PAUSED"
print(CreateOrEditJob(template))

# COMMAND ----------


