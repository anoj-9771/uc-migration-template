# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

clusterId = GetClusterIdByName("riverwatch")
poolId = GetPoolIdByName("RIVERWATCH_POOL")
pipeline = {
    "name": "riverwatch-pipeline",
    "run_as": {
        "service_principal_name": ""
    },
    "email_notifications": {
        "no_alert_for_skipped_runs": False
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "preprocess_water_quality_features",
            "notebook_task": {
                "notebook_path": "/AI-ML/Riverwatch/01_water_quality_feature_selection",
                "base_parameters": {
                    "current_model_runtime": "",
                    "last_model_runtime": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "inference_water_quality",
            "depends_on": [
                {
                    "task_key": "preprocess_water_quality_features"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Riverwatch/02_water_quality_predictions",
                "base_parameters": {
                    "current_model_runtime": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "riverwatch-job-cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "12.2.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "True"
                },
                "instance_pool_id": poolId,
                "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                "runtime_engine": "STANDARD",
                "num_workers": 2
            }
        }
    ],
    "format": "MULTI_TASK"
}
print(CreateOrEditJob(pipeline))
