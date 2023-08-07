# Databricks notebook source
# MAGIC %run ../../Common/common-workspace

# COMMAND ----------

clusterId = GetClusterIdByName("sewer-cctv")
poolId = GetPoolIdByName("SCCTV_POOL")
sewer_cctv_pipeline = {
    "name": "sewer-cctv-pipeline",
    "run_as": {
        "service_principal_name": ""
    },
    "email_notifications": {
        "no_alert_for_skipped_runs": False
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 5,
    "tasks": [
        {
            "task_key": "extract_video_metadata",
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/001_extract_video_metadata",
                "base_parameters": {
                    "video_id": "",
                    "priority": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "libraries": [
                {
                    "pypi": {
                        "package": "opencv-python==4.6.0.66"
                    }
                }
            ],
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "save_cctv_images",
            "depends_on": [
                {
                    "task_key": "extract_video_metadata"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/002_save_cctv_images",
                "base_parameters": {
                    "video_id": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "libraries": [
                {
                    "pypi": {
                        "package": "opencv-python==4.6.0.66"
                    }
                }
            ],
            "max_retries": 2,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "classify_sewer_defects_in_images",
            "depends_on": [
                {
                    "task_key": "save_cctv_images"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/006_classify_sewer_defects_in_images",
                "base_parameters": {
                    "video_id": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "libraries": [
                {
                    "pypi": {
                        "package": "mlflow==1.27.0"
                    }
                },
                {
                    "whl": "dbfs:/mnt/blob-sewercctvmodel/MainModel-0.1.0-py3-none-any.whl"
                }
            ],
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": True,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "read_text_in_images",
            "depends_on": [
                {
                    "task_key": "save_cctv_images"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/003_read_text_in_images",
                "base_parameters": {
                    "video_id": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.microsoft.azure:synapseml_2.12:0.9.5",
                        "repo": "https://mmlspark.azureedge.net/maven"
                    }
                }
            ],
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "extract_contractor_annotations_and_distance_strings",
            "depends_on": [
                {
                    "task_key": "read_text_in_images"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/004_extract_contractor_annotations_and_distance_strings",
                "base_parameters": {
                    "video_id": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "cluster_contractor_identified_defects_across_video",
            "depends_on": [
                {
                    "task_key": "extract_contractor_annotations_and_distance_strings"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/005_cluster_contractor_identified_defects_across_video",
                "base_parameters": {
                    "video_id": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "cluster_AI_identified_defects",
            "depends_on": [
                {
                    "task_key": "classify_sewer_defects_in_images"
                },
                {
                    "task_key": "extract_contractor_annotations_and_distance_strings"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/007_cluster_AI_identified_defects",
                "base_parameters": {
                    "video_id": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "archive_video",
            "depends_on": [
                {
                    "task_key": "cluster_contractor_identified_defects_across_video"
                },
                {
                    "task_key": "cluster_AI_identified_defects"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/008_archive_video",
                "base_parameters": {
                    "video_id": "",
                    "priority": ""
                },
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            }
        },
        {
            "task_key": "Save_Data_To_Azure_SQL",
            "depends_on": [
                {
                    "task_key": "archive_video"
                }
            ],
            "notebook_task": {
                "notebook_path": "/AI-ML/Sewer CCTV/009_save_to_azure_SQL",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": clusterId,
            "max_retries": 0,
            "min_retry_interval_millis": 15000,
            "retry_on_timeout": False,
            "timeout_seconds": 3600,
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
            "job_cluster_key": "sewercctv-job-cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "10.4.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "True"
                },
                "instance_pool_id": poolId,
                "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                "runtime_engine": "STANDARD",
                "num_workers": 3
            }
        }
    ],
    "format": "MULTI_TASK"
}
print(CreateOrEditJob(sewer_cctv_pipeline))
