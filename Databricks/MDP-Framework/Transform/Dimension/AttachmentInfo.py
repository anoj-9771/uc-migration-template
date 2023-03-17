# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    crmorderphf_df = GetTable(f"{SOURCE}.crm_crmorderphf").selectExpr("documentFileName as fileName","documentType as fileType","documentFileSize as fileSize","documentID as documentId")
    crmorderphio_df = GetTable(f"{SOURCE}.crm_crmorderphio").selectExpr("createdByUser","creationDatetime","changedByUser","changeDateTime","documentID")
    aurion_employee_df = spark.sql(f"""Select userid, givenNames, surname from {SOURCE}.aurion_active_employees union Select userid, givenNames, surname from {SOURCE}.aurion_terminated_employees""")
    # ------------- JOINS ------------------ #
    df = (
        crmorderphf_df.join(crmorderphio_df,"documentID","left")
        .join(aurion_employee_df,crmorderphio_df.createdByUser == aurion_employee_df.userid,"left")
        .withColumn("createdBy",coalesce(concat("givenNames", lit(" "),"surname"), "createdByUser")).drop("userid","givenNames","surname")
        .join(aurion_employee_df,crmorderphio_df.changedByUser == aurion_employee_df.userid,"left")
        .withColumn("modifiedBy",coalesce(concat("givenNames", lit(" "), "surname"), "changedByUser")).drop("userid","givenNames","surname")
        .selectExpr("fileName", "fileType", "fileSize", "documentId", "createdByUser",
                    "createdBy",
                    "creationDatetime",
                    "changedByUser",
                    "modifiedBy",
                    "changeDateTime")        
    )    
    # ------------- TRANSFORMS ------------- #
        
    _.Transforms = [
         f"documentId {BK}"
        ,"fileName"
        ,"fileType"
        ,"fileSize"
        ,"documentId"
        ,"createdBy"
        ,"createdByUser createdByUserID"
        ,"creationDatetime createdDateTime"
        ,"modifiedBy"
        ,"changedByUser modifiedByUserID"
        ,"changeDateTime modifiedDateTime"
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


