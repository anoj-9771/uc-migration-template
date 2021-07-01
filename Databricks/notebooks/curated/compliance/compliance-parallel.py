# Databricks notebook source
# MAGIC %run ../../includes/util-parallel-exec

# COMMAND ----------

notebooks = [
 NotebookData("./compliance-curated", 0, {"param_load_dimensions":"true", "param_load_facts":"false", "param_refresh_link_sp":"false"}),
 NotebookData("../compliance/functions/facts/functions/unit-enrolment-links", 0),
 NotebookData("../compliance/functions/facts/functions/course-enrolment-links", 0)
]


# COMMAND ----------

res = parallelNotebooks(notebooks, 3)
result = [f.result(timeout=0) for f in res] # This is a blocking call.
print(result)

# COMMAND ----------

if result[0] == "1" and result[1] == "1" and result[2] == "1":
  dbutils.notebook.run("./compliance-curated", 0, {"param_load_dimensions":"false", "param_load_facts":"true", "param_refresh_link_sp":"false"})
