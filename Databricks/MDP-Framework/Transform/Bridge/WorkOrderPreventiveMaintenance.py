# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    
    # ------------- TABLES ----------------- #
    df = spark.sql(f"""
                    select fwo.workOrderSK,fwo.workOrderCreationID,fwo.workOrderChangeTimestamp, fpm.preventiveMaintenanceSK,fpm.preventiveMaintenanceID,fpm.preventiveMaintenanceChangedTimestamp from {get_table_namespace(f'{TARGET}', 'factWorkOrder')} fwo
                    inner join {get_table_name(f"{SOURCE}","maximo","workOrder")} wo on fwo.workOrderCreationId = wo.workOrder
                    inner join {get_table_name(f"{SOURCE}","maximo","pM")} pm on wo.pM = pm.pM
                    inner join {get_table_namespace(f'{TARGET}', 'factpreventivemaintenance')} fpm on fpm.preventiveMaintenanceID = pm.pM
                    where wo._RecordDeleted = 0 and pm._RecordDeleted = 0 and fpm._recordCurrent =1 and fpm._recordDeleted = 0 and fwo._recordCurrent =1 and fwo._recordDeleted = 0
                   """).drop_duplicates()
      
                                
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
