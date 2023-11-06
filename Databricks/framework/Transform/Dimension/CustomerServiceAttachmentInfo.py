# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

###cleansed layer table (cleansed.crm.crmorderphf / cleansed.crm.crmorderphio ) is full reload ###
crmorderphf_df = spark.sql(f""" SELECT documentFileName as customerServiceAttachmentFileName
                          ,documentType as customerServiceAttachmentFileType
                          ,documentFileSize as customerServiceAttachmentFileSize
                          ,documentID as customerServiceAttachmentDocumentId
                      FROM {getEnv()}cleansed.crm.crmorderphf  """)

crmorderphio_df = spark.sql(f""" SELECT createdByUser as customerServiceAttachmentCreatedByUserId
                          ,creationDatetime as customerServiceAttachmentCreatedTimestamp
                          ,changedByUser as customerServiceAttachmentModifiedByUserId
                          ,changeDateTime as customerServiceAttachmentModifiedTimestamp
                          ,documentID 
                      FROM {getEnv()}cleansed.crm.crmorderphio  """)

aurion_df =       spark.sql(f"""SELECT userid, givenNames, surname 
                                 FROM {getEnv()}cleansed.aurion.active_employees
                               UNION
                               SELECT userid, givenNames, surname
                                 FROM {getEnv()}cleansed.aurion.terminated_employees """)

#avoid duplicates in Aurion/ Pick first record ##
aurion_df = aurion_df.withColumn("uniqueId", monotonically_increasing_id())
windowStatement = Window.partitionBy("userid").orderBy(col("uniqueId").desc())
aurion_df = aurion_df.withColumn("row_number", row_number().over(windowStatement))  
aurion_df = aurion_df.filter(col("row_number") == 1).drop("row_number", "uniqueId")  

# COMMAND ----------

# ------------- JOINS & SELECT ------------------ #
df = (crmorderphf_df
        .join(crmorderphio_df, crmorderphf_df.customerServiceAttachmentDocumentId  == crmorderphio_df.documentID, "left")
        .join(aurion_df,crmorderphio_df.customerServiceAttachmentCreatedByUserId == aurion_df.userid,"left")
        .withColumn("customerServiceAttachmentCreatedByUserName",
                    coalesce(concat("givenNames", lit(" "),"surname"), "customerServiceAttachmentCreatedByUserId")).drop("userid","givenNames","surname")
        .join(aurion_df,crmorderphio_df.customerServiceAttachmentModifiedByUserId == aurion_df.userid,"left")
        .withColumn("customerServiceAttachmentModifiedByUserName",
                    coalesce(concat("givenNames", lit(" "), "surname"), "customerServiceAttachmentModifiedByUserId")).drop("userid","givenNames","surname")
        .select(col("customerServiceAttachmentDocumentId").alias(f"{BK}")
               ,col("customerServiceAttachmentFileName")
               ,col("customerServiceAttachmentFileType")
               ,col("customerServiceAttachmentFileSize")
               ,col("customerServiceAttachmentDocumentId")
               ,col("customerServiceAttachmentCreatedByUserName")
               ,col("customerServiceAttachmentCreatedByUserId")
               ,col("customerServiceAttachmentCreatedTimestamp")
               ,col("customerServiceAttachmentModifiedByUserName")
               ,col("customerServiceAttachmentModifiedByUserId")
               ,col("customerServiceAttachmentModifiedTimestamp")
               )
        )

if not(TableExists(_.Destination)):
    df = df.unionByName(spark.createDataFrame([dummyRecord(df.schema)], df.schema))
#CleanSelf()
Save(df)
#SaveWithCDF(df, 'Full Load')
