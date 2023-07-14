# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ./Workflows/rbac-assignment

# COMMAND ----------

def CreateWorkflow(template):
    template["run_as"]["service_principal_name"] = GetServicePrincipalId()
    template["job_clusters"][0]["new_cluster"]["instance_pool_id"] = GetPoolIdByName("pool-small")
    template["job_clusters"][0]["new_cluster"]["driver_instance_pool_id"] = GetPoolIdByName("pool-small")
    print(CreateJob(template))

# COMMAND ----------

CreateWorkflow(rbac_assignment)

# COMMAND ----------


