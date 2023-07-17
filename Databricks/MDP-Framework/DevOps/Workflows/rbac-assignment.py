# Databricks notebook source
rbac_assignment = {
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
        "quartz_cron_expression": "0 00 8-17 ? * MON-FRI *",
        "timezone_id": "Australia/Sydney",
        "pause_status": "UNPAUSED"
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
                "spark_version": "13.1.x-scala2.12",
                "azure_attributes": {},
                "instance_pool_id": "",
                "driver_instance_pool_id": "",
                "data_security_mode": "USER_ISOLATION",
                "runtime_engine": "STANDARD",
                "num_workers": 1
            }
        }
    ],
    "format": "MULTI_TASK"
}