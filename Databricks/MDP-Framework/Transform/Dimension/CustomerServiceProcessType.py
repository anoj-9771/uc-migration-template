# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0crm_proc_type_text')}")
#     Linking table to connect proc_type to object_type
    proc_type_df = GetTable(f"{SOURCE}.crm_crmc_proc_type").select("processTypeCode","objectTypeCode")
#    Select objectTypeDescription from crm_crmc_prt_otype such that there is only 1 record for each objectTypeCode 
    object_df = spark.sql(
        f"""SELECT objectTypeCode, objectTypeDescription FROM 
           ((SELECT objectTypeCode, objectTypeDescription, 1 AS record_number FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} WHERE objectTypeCode IN 
            (SELECT objectTypeCode FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} GROUP BY objectTypeCode HAVING COUNT(1) = 1))
           UNION
            (SELECT objectTypeCode, objectTypeDescription, ROW_NUMBER() OVER(PARTITION BY objectTypeCode ORDER BY extract_datetime DESC) AS record_number FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} WHERE objectTypeCode IN 
            (SELECT objectTypeCode FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} GROUP BY objectTypeCode HAVING COUNT(1) > 1) and DefaultBorType = 'X')
            ) where record_number = 1""")
    process_usage_pref1 = spark.sql(f"""select distinct processTypeCode as pc1 from {SOURCE}.crm_0crm_srv_req_inci_h where processTypeCode is not null""")
    process_usage_pref2 = spark.sql(f"""select distinct processTypeCode as pc2 from {SOURCE}.crm_0crm_sales_act_1 where processTypeCode is not null""")
    # ------------- JOINS ------------------ #
    df = df.join(proc_type_df,"processTypeCode","inner") \
    .join(process_usage_pref1,df.processTypeCode == process_usage_pref1.pc1,"left") \
    .join(process_usage_pref2,df.processTypeCode == process_usage_pref2.pc2,"left") \
    .join(object_df,"objectTypeCode","left") \
    .withColumn("sourceSystemCode", lit("CRM"))
    
    df = df.withColumn("processTypeUsage",  when( df.pc1.isNotNull(), lit("Service Request"))
                       .when(df.pc2.isNotNull(), lit("Interaction"))
                       .otherwise(df.objectTypeDescription))
    
    df = df.na.drop(subset =["processTypeUsage"],how = "all")
    df = df.where("processTypeUsage == 'Interaction' or processTypeUsage == 'Service Request' "  )
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"processTypeCode||'|'||sourceSystemCode {BK}"
        ,"processTypeCode customerServiceProcessTypeCode"
        ,"processTypeShortDescription customerServiceProcessTypeShortDescription"
        ,"processTypeDescription customerServiceProcessTypeDescription"
        ,"processTypeUsage customerServiceProcessTypeUsageName"
        ,"sourceSystemCode sourceSystemCode"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#    display(df)
#    CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
