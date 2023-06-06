# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, monotonically_increasing_id

def Transform():
    # ------------- TABLES ----------------- #
    crmorderphf_df = GetTable(f"{SOURCE}.crm_crmorderphf").selectExpr("documentFileName as fileName","documentType as fileType","documentFileSize as fileSize","documentID as documentId")
    crmorderphio_df = GetTable(f"{SOURCE}.crm_crmorderphio").selectExpr("createdByUser","creationDatetime","changedByUser","changeDateTime","documentID")
    aurion_employee_df = spark.sql(f"""Select userid, givenNames, surname from {SOURCE}.aurion_active_employees union Select userid, givenNames, surname from {SOURCE}.aurion_terminated_employees""")
    
    aurion_employee_df = aurion_employee_df.withColumn("uniqueId", monotonically_increasing_id())
    windowStatement = Window.partitionBy("userid").orderBy(col("uniqueId").desc())
    aurion_employee_df = aurion_employee_df.withColumn("row_number", row_number().over(windowStatement))  #.filter("userid == 'A1H' ")
    aurion_employee_df = aurion_employee_df.filter(col("row_number") == 1).drop("row_number", "uniqueId")  # .filter("userid == 'A1H' ")

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
        ,"fileName          customerServiceAttachmentFileName"
        ,"fileType          customerServiceAttachmentFileType"
        ,"fileSize          customerServiceAttachmentFileSize"
        ,"documentId        customerServiceAttachmentDocumentId"
        ,"createdBy         customerServiceAttachmentCreatedByUserName"
        ,"createdByUser     customerServiceAttachmentCreatedByUserId"
        ,"creationDatetime  customerServiceAttachmentCreatedTimestamp"
        ,"modifiedBy        customerServiceAttachmentModifiedByUserName"
        ,"changedByUser     customerServiceAttachmentModifiedByUserId"
        ,"changeDateTime    customerServiceAttachmentModifiedTimestamp"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()

