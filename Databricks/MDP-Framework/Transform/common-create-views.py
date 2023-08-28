# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

# MAGIC %run ../Common/common-jdbc

# COMMAND ----------

# MAGIC %run ../Common/common-helpers

# COMMAND ----------

task = dbutils.widgets.get("task")
j = json.loads(task)
systemCode = j.get("SystemCode").lower()
systemCodeCleaned = systemCode

if systemCodeCleaned[-3:] == 'ref': 
    systemCodeCleaned = systemCode[:len(systemCodeCleaned)-3]
if systemCodeCleaned[-4:] == 'data':
    systemCodeCleaned = systemCode[:len(systemCodeCleaned)-4]
if "|" in systemCodeCleaned:
    systemCodeCleaned = systemCode.split("|")[0]
    
print(systemCode)
print(systemCodeCleaned)

# COMMAND ----------

#run notebook if exists passing through 
basePath = "/MDP-Framework/Transform/Views/"
notebookName = f"{systemCodeCleaned}-view-creation"
notebookPath = f"{basePath}{notebookName}"
df = JsonToDataFrame(ListWorkspaces(basePath))
df = ExpandTable(df)

#if exists run notebook otherwise return
#Execute cleaned system code notebook path but pass through original system code
if df.where(f"lower(objects_path) = '{notebookPath.lower()}'").count() > 0:
    print(f"Notebook {notebookName} exists! Running now...")
    dbutils.notebook.run(notebookPath, 0, {"system_code":f"{systemCode}"})
    print("Notebook run complete.")
else:
    print(f"Notebook {notebookPath} does not exist.")

# COMMAND ----------

#Below sets the common-create-views to not run for a given system code.Update value to 1 for your system code to re run and create views via ADF
#update flag for original system code
ExecuteStatement(f"""
update dbo.config set value = 0
where keyGroup = 'RunViewCreation' and [Key] = '{systemCode}' and value = 1
""")

