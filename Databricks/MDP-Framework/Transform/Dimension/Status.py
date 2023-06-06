# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_tj30t')}")
    proc_type_df = GetTable(f"{SOURCE}.crm_crmc_proc_type").select("objectTypeCode","userStatusProfile").dropDuplicates()
    object_df = spark.sql(
        f"""SELECT objectTypeCode, objectTypeDescription FROM 
           ((SELECT objectTypeCode, objectTypeDescription, 1 AS record_number FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} WHERE objectTypeCode IN 
            (SELECT objectTypeCode FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} GROUP BY objectTypeCode HAVING COUNT(1) = 1))
           UNION
            (SELECT objectTypeCode, objectTypeDescription, ROW_NUMBER() OVER(PARTITION BY objectTypeCode ORDER BY extract_datetime DESC) AS record_number FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} WHERE objectTypeCode IN 
            (SELECT objectTypeCode FROM {get_table_namespace(f'{SOURCE}', 'crm_crmc_prt_otype')} GROUP BY objectTypeCode HAVING COUNT(1) > 1) and DefaultBorType = 'X')
            ) where record_number = 1""")
    status_usage_pref1 = spark.sql(f"""select distinct statusProfile as sp1 from {SOURCE}.crm_0crm_srv_req_inci_h where statusProfile is not null""")
    status_usage_pref2 = spark.sql(f"""select distinct statusProfile as sp2 from {SOURCE}.crm_0crm_sales_act_1 where statusProfile is not null""")
    # ------------- JOINS ------------------ #
    df = df.join(proc_type_df,df.statusProfile == proc_type_df.userStatusProfile,"inner") \
        .join(status_usage_pref1,(df.statusProfile == status_usage_pref1.sp1),"left") \
        .join(status_usage_pref2,(df.statusProfile == status_usage_pref2.sp2),"left") \
        .join(object_df,"objectTypeCode","left")
    df = df.withColumn("statusUsage",  when( df.sp1.isNotNull(), lit("Service Request"))
                       .when(df.sp2.isNotNull(), lit("Interaction"))
                       .otherwise(df.objectTypeDescription))
    df = df.na.drop(subset =["statusUsage"],how = "all")
    

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"objectTypeCode||'|'||statusProfile||'|'|| statusCode {BK}"
        ,"statusProfile statusProfile"
        ,"statusCode statusCode"
        ,"statusShortDescription statusShortDescription"
        ,"status status"
        ,"objectTypeCode objectTypeCode"
        ,"statusUsage statusUsage"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#     CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
