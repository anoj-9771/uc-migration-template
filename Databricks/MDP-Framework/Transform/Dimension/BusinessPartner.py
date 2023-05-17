# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |17/05/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

from pyspark.sql.functions import row_number, col, lit, when
from pyspark.sql import Window

isu_0bpartner_attr_df = (GetTable(f"{SOURCE}.isu_0bpartner_attr")
                                    .filter((col("businessPartnerCategoryCode").isin("1", "2")) & 
                                            (col("_RecordCurrent")== 1))
                                    .withColumn("priority", lit(0).cast("int"))
                                    .withColumn("sourceSystemCode",lit("ISU")) 
                                    .withColumn("warWidowFlag",lit("N"))
                                    .withColumn("deceasedFlag",lit("N"))
                                    .withColumn("disabilityFlag",lit("N"))
                                    .withColumn("goldCardHolderFlag",lit("N"))
                                    .withColumn("consent1Indicator",lit(None).cast(StringType()))
                                    .withColumn("consent2Indicator",lit(None).cast(StringType()))
                                    .withColumn("eligibilityFlag",lit("N"))
                                    .withColumn("plannedChangeDocument",lit(None).cast(StringType()))
                                    .withColumn("paymentStartDate",lit(None).cast(StringType()))
                                    .withColumn("dateOfCheck",lit(None).cast(StringType()))
                                    .withColumn("pensionConcessionCardFlag",lit("N"))
                                    .withColumn("pensionType",lit(None).cast(StringType()))
                                            .select( col("sourceSystemCode")
                                                    ,col("businessPartnerNumber")
                                                    ,col("businessPartnerCategoryCode")
                                                    ,col("businessPartnerCategory")                               
                                                    ,col("businessPartnerTypeCode")                                
                                                    ,col("businessPartnerType")                                  
                                                    ,col("businessPartnerGroupCode")                           
                                                    ,col("businessPartnerGroup")                              
                                                    ,col("externalBusinessPartnerNumber").alias("externalNumber") 
                                                    ,col("businessPartnerGUID")                            
                                                    ,col("firstName")   
                                                    ,col("lastName")                                        
                                                    ,col("middleName") 
                                                    ,col("nickName")   
                                                    ,col("titleCode")         
                                                    ,col("title")                                           
                                                    ,col("dateOfBirth")                                         
                                                    ,col("dateOfDeath")                                            
                                                    ,col("validFromDate")                                     
                                                    ,col("validToDate")                                           
                                                    ,col("personNumber")                                          
                                                    ,col("personnelNumber")                                        
                                                    ,col("organizationName")                                     
                                                    ,col("organizationFoundedDate")               
                                                    ,col("createdDateTime")                                  
                                                    ,col("createdBy") 
                                                    ,col("lastUpdatedDateTime")                            
                                                    ,col("lastUpdatedBy")                                    
                                                    ,col("naturalPersonFlag")                              
                                                    ,col("_RecordDeleted")
                                                    ,col("warWidowFlag") 
                                                    ,col("deceasedFlag")         
                                                    ,col("disabilityFlag")             
                                                    ,col("goldCardHolderFlag")                                
                                                    ,col("consent1Indicator")                 
                                                    ,col("consent2Indicator")                                 
                                                    ,col("eligibilityFlag")                                   
                                                    ,col("plannedChangeDocument")          
                                                    ,col("paymentStartDate")                                     
                                                    ,col("dateOfCheck")
                                                    ,col("pensionConcessionCardFlag")                            
                                                    ,col("pensionType")
                                                    ,col("priority")
                                                    )
                        )


#isu_0bpartner_attr_df.display()

crm_0bpartner_attr_df = (GetTable(f"{SOURCE}.crm_0bpartner_attr")
                                            .filter((col("businessPartnerCategoryCode").isin("1", "2")) & 
                                            (col("_RecordCurrent")== 1) & (col("_RecordDeleted")== 0))
                                            .withColumn("priority", lit(1).cast("int"))
                                            .withColumn("sourceSystemCode",lit("CRM"))
                                            .select( col("sourceSystemCode")
                                                    ,col("businessPartnerNumber")
                                                    ,col("businessPartnerCategoryCode")
                                                    ,col("businessPartnerCategory")                               
                                                    ,col("businessPartnerTypeCode")                                
                                                    ,col("businessPartnerType")                                  
                                                    ,col("businessPartnerGroupCode")                           
                                                    ,col("businessPartnerGroup")                              
                                                    ,col("externalBusinessPartnerNumber").alias("externalNumber") 
                                                    ,col("businessPartnerGUID")                            
                                                    ,col("firstName")   
                                                    ,col("lastName")                                        
                                                    ,col("middleName") 
                                                    ,col("nickName")   
                                                    ,col("titleCode")         
                                                    ,col("title")                                           
                                                    ,col("dateOfBirth")                                         
                                                    ,col("dateOfDeath")                                            
                                                    ,col("validFromDate")                                     
                                                    ,col("validToDate")                                           
                                                    ,col("personNumber")                                          
                                                    ,col("personnelNumber")                                        
                                                    ,col("organizationName")                                     
                                                    ,col("organizationFoundedDate")                
                                                    ,col("createdDateTime")                                  
                                                    ,col("createdBy")                                            
                                                    ,col("lastUpdatedDateTime")                            
                                                    ,col("lastUpdatedBy") 
                                                    ,col("naturalPersonFlag")                              
                                                    ,col("_RecordDeleted") 
                                                    ,col("warWidowFlag") 
                                                    ,col("deceasedFlag")         
                                                    ,col("disabilityFlag")             
                                                    ,col("goldCardHolderFlag")                                
                                                    ,col("consent1Indicator")                 
                                                    ,col("consent2Indicator")                                 
                                                    ,col("eligibilityFlag")                                   
                                                    ,col("plannedChangeDocument")          
                                                    ,col("paymentStartDate")                                     
                                                    ,col("dateOfCheck")
                                                    ,col("pensionConcessionCardFlag")                            
                                                    ,col("pensionType")
                                                    ,col("priority")
                                                    )
                         )

#crm_0bpartner_attr_df.display()

aurion_df = (GetTable(f"{SOURCE}.vw_aurion_employee_details")
                    .withColumn("sourceSystemCode",lit("AURION"))
                    .withColumn("priority", when(col("aurionfilename") == "active", 0)
                                            .when(col("aurionfilename") == "history", 1)
                                            .when(col("aurionfilename") == "terminated", 2)
                                            .otherwise(3))
                    .withColumn("businessPartnerCategory",lit("Person"))
                    .withColumn("businessPartnerCategoryCode",lit("1"))
                    .withColumn("businessPartnerGroup",lit("Employee"))
                    .withColumn("businessPartnerGroupCode",lit("ZE"))
                                            .select( col("sourceSystemCode")
                                                    ,col("businessPartnerNumber")
                                                    ,col("businessPartnerCategoryCode")
                                                    ,col("businessPartnerCategory")                               
                                                    ,col("businessPartnerGroupCode")                           
                                                    ,col("businessPartnerGroup")                              
                                                    ,col("givenNames").alias("firstName")   
                                                    ,col("surname").alias("lastName")                                        
                                                    ,col("personNumber")                                          
                                                    ,col("employeeNumber")
                                                    ,col("UserID").alias("userId")
                                                    ,col("dateCommenced")
                                                    ,col("EmployeeStatus").alias("employeeStatus") 
                                                    ,col("priority")
                                                    )
                         )
                         

# COMMAND ----------

#ISU priority then CRM
maindf = isu_0bpartner_attr_df.unionByName(crm_0bpartner_attr_df)
windowSpec = Window.partitionBy("businessPartnerNumber").orderBy("priority")
maindf = maindf.withColumn("selectInd", row_number().over(windowSpec))
maindf = maindf.filter(col("selectInd") == 1).drop("selectInd", "priority")

#Aurion pick single record in priority (active/terminated/history)
auriondf = aurion_df.withColumn("selectInd", row_number().over(windowSpec))
auriondf = auriondf.filter(col("selectInd") == 1).drop("selectInd", "priority")
mainCols = set(maindf.columns)
aurCols  = set(auriondf.columns)

#now upsert maindf with aurion df
upsertdf = (maindf.alias("mdf").join(
      auriondf.alias("adf"), 
       col("mdf.businessPartnerNumber") ==  col("adf.businessPartnerNumber"), how='left')
     )


#df.display()
###column Selects ##
exprSelect = [ when (col(f"mdf.{col_name}").isNotNull(),  
                     col(f"mdf.{col_name}")).otherwise(col(f"adf.{col_name}")).alias(col_name)
               if col_name in aurCols else col(f"mdf.{col_name}")  
               for col_name in maindf.columns     
             ]
exprSelect += [col(f"adf.{col_name}").alias(col_name) for col_name in aurCols if col_name not in mainCols]
##################

finaldf = upsertdf.select(*exprSelect)

# COMMAND ----------

####USE THE BELOW ONLY FOR FIRST TIME PROD RUN DELTA VARIATIONS BETWEEN ENV'S##########################################
upsertdf2 = (maindf.alias("mdf").join(
      auriondf.alias("adf"), 
       col("mdf.businessPartnerNumber") ==  col("adf.businessPartnerNumber"), how='full')
     )
initDeltaDF = upsertdf2.filter(col("mdf.businessPartnerNumber").isNull())
deltaCount = initDeltaDF.count()
exprDSelect = [ when (col(f"mdf.{col_name}").isNotNull(),  
                     col(f"mdf.{col_name}")).otherwise(col(f"adf.{col_name}")).alias(col_name)
               if col_name in aurCols else col(f"mdf.{col_name}")  
               for col_name in maindf.columns     
             ]
exprDSelect += [col(f"adf.{col_name}").alias(col_name) for col_name in aurCols if col_name not in mainCols]
finitDeltaDF = initDeltaDF.select(*exprDSelect)
##################NO DELTA EXPECTED AFTER FIRST RUN ###################################
#########################################################################################

# COMMAND ----------

def Transform():
    global df    
    business_key = "businessPartnerNumber"
    if deltaCount = 0 or deltaCount is None:
        deltaExist = False
        df = finaldf
    else:
        df = finitDeltaDF
        deltaExist = True



    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
         f"{business_key} {BK}"
        ,'*'
    ]

    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    #display(df)
    #CleanSelf()
    if isSchemaChanged(currentDataFrame):
        saveSchemaAndData(finaldf, 'businessPartnerNumber')
        if deltaExist:
            Save(df)
    else:
        Save(df)
    #DisplaySelf()
Transform()
