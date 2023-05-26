# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    
    # ------------- TABLES ----------------- #
    df = spark.sql(f"""
                    select fwo.workOrderSK,fwo.workOrderCreationID,fwo.workOrderChangeTimestamp, fpm.preventiveMaintenanceSK,fpm.preventiveMaintenanceID,fpm.preventiveMaintenanceChangedTimestamp from {TARGET}.factWorkOrder fwo
                    inner join {get_table_name(f"{SOURCE}","maximo","workOrder")} wo on fwo.workOrderCreationId = wo.workOrder
                    inner join {get_table_name(f"{SOURCE}","maximo","pM")} pm on wo.pM = pm.pM
                    inner join {TARGET}.factpreventivemaintenance fpm on fpm.preventiveMaintenanceID = pm.pM
                   """)
      
                                
    # ------------- JOINS ------------------ #
   
    
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"workOrderSK||'|'||preventiveMaintenanceSK {BK}"
        ,"workOrderSK workOrderFK"
        ,"preventiveMaintenanceSK preventiveMaintenanceFK"
        ,"workOrderCreationID"
        ,"workOrderChangeTimestamp"
        ,"preventiveMaintenanceID"
        ,"preventiveMaintenanceChangedTimestamp"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()

# COMMAND ----------


