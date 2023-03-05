# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |28/02/2023 |Mag          |Transform cleansed.maximo_sr to Curated_v2.factServiceTicket 

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df    
    global table1
    global table2
    global table3
    global transformQuery
    
    # ------------- TABLES ----------------- #   
    table1 = GetTableName(f"{SOURCE}.maximo_relatedrecord")
    #table2 = GetTableName(f"{DEFAULT_TARGET}.factWorkOrder")
    #table3 = GetTableName(f"{DEFAULT_TARGET}.factserviceTicket")
    
    # ------------- JOIN AND SELECT ----------------- #
    transformQuery = f"""SELECT recordKey workOrderSK, relatedRecordKey serviceTicketSK, relatedRecordClass, relatedRecordKey, recordKey 
                         FROM {table1}  
                         WHERE class = 'WORKORDER' and relatedRecordClass = 'SR'
                     """
    
     #transformQuery = f"""SELECT bb.workOrderSK, cc.serviceTicketSK, aa.relatedRecordClass , aa.relatedRecordKey, aa.recordKey 
     #                    FROM {table1}  aa 
     #                    LEFT JOIN {table2} bb ON aa.recordKey = bb.workOrderNumber 
     #                    LEFT JOIN {table3} cc ON aa.relatedRecordKey = cc.serviceRequest 
     #                    WHERE aa.class = 'WORKORDER' and aa.relatedRecordClass = 'SR'
     #                """
    
    df = spark.sql(transformQuery).cache()
    
     #------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"workOrderSK||'|'||serviceTicketSK {BK}" 
        ,"workOrderSK workOrderFK"
        ,"serviceTicketSK serviceTicketFK"
        ,"relatedRecordClass relatedRecordClass"
        ,"relatedRecordKey ticketClass"
        ,"recordKey recordKey"
        
    ]
    
    df = df.selectExpr(
        _.Transforms
    )
    
    #df.display()
    #display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
    
Transform()
