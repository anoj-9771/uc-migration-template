# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#####Determine Load #################
##Move this to controlDB config if its not complex to derive the change columns needed####

changeColumnsISU = ["businessPartnerNumber", "businessPartnerCategoryCode", "businessPartnerCategory"                              
                 ,"businessPartnerTypeCode","businessPartnerType" ,"businessPartnerGroupCode"                           
                 ,"businessPartnerGroup" ,"externalBusinessPartnerNumber","businessPartnerGUID"                           
                 ,"firstName" ,"lastName"  ,"middleName" ,"nickName" ,"titleCode","title"                                           
                 ,"dateOfBirth" , "dateOfDeath", "validFromDate" ,"validToDate","personNumber"                                          
                 ,"personnelNumber","organizationName","organizationName1","organizationName2"                                     
                 ,"organizationFoundedDate", "createdDateTime", "createdBy","lastUpdatedDateTime"                           
                 ,"lastUpdatedBy" ,"naturalPersonFlag","_RecordDeleted"]

changeColumnsCRM = ["businessPartnerNumber", "businessPartnerCategoryCode", "businessPartnerCategory"                              
                  ,"businessPartnerTypeCode","businessPartnerType" ,"businessPartnerGroupCode"                           
                  ,"businessPartnerGroup" ,"externalBusinessPartnerNumber","businessPartnerGUID"                           
                  ,"firstName" ,"lastName"  ,"middleName" ,"nickName" ,"titleCode","title"                                           
                  ,"dateOfBirth" , "dateOfDeath", "validFromDate" ,"validToDate","personNumber"                                          
                  ,"personnelNumber","organizationName","organizationName1","organizationName2"                                     
                  ,"organizationFoundedDate", "createdDateTime", "createdBy","lastUpdatedDateTime"                           
                  ,"lastUpdatedBy" ,"naturalPersonFlag","_RecordDeleted"]

###############################
driverTable1 = 'cleansed.isu.0bpartner_attr'   
driverTable2 = 'cleansed.crm.0bpartner_attr' 

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}").withColumn("_change_type", lit(None))
    derivedDF2 = GetTable(f"{getEnv()}{driverTable2}").withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, changeColumnsISU, True).filter(col("_change_type").rlike('update_postimage|insert'))#.drop(col("_change_type"))
    derivedDF2 = getSourceCDF(driverTable2, changeColumnsCRM, True).filter(col("_change_type").rlike('update_postimage|insert'))#.drop(col("_change_type"))
    if derivedDF1.count() == 0 and derivedDF2.count() == 0:
        print("No delta to be  processed")

# COMMAND ----------

from pyspark.sql.functions import row_number, col, lit, when
from pyspark.sql import Window

isuDF = (derivedDF1
                                    .filter((col("businessPartnerCategoryCode").isin("1", "2")) & 
                                            (col("_RecordCurrent")== 1))  
                                    .withColumn("sourceSystemCode",lit("ISU"))                                   
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
                                                    ,col("organizationName1")
                                                    ,col("organizationName2")                                     
                                                    ,col("organizationFoundedDate")               
                                                    ,col("createdDateTime")                                  
                                                    ,col("createdBy") 
                                                    ,col("lastUpdatedDateTime")                            
                                                    ,col("lastUpdatedBy")                                    
                                                    ,col("naturalPersonFlag")                              
                                                    ,col("_RecordDeleted")                                                                                                        
                                                    ,col("_change_type"))
                        )


crmDF = (derivedDF2
                                            .filter((col("businessPartnerCategoryCode").isin("1", "2")) & 
                                            (col("_RecordCurrent")== 1) & (col("_RecordDeleted")== 0))                                            
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
                                                    ,col("organizationName1")
                                                    ,col("organizationName2")                                     
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
                                                    ,col("_change_type"))
                         )



aurDF = (GetTable(f"{getEnv()}cleansed.aurion.employee_details")
                    .withColumn("sourceSystemCode",lit("AURION"))
                    .withColumn("priority", when(col("aurionfilename") == "active", 0)
                                            .when(col("aurionfilename") == "terminated", 1)
                                            .when(col("aurionfilename") == "history", 2)
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

################Choose and merge columns for ISU AND CRM #########################################
crmOnlyFlagColumns = ["warWidowFlag", "deceasedFlag", "disabilityFlag", "goldCardHolderFlag", "eligibilityFlag", "pensionConcessionCardFlag"]
crmOnlyColumns = ["consent1Indicator","consent2Indicator","plannedChangeDocument", "paymentStartDate", "dateOfCheck", "pensionType"]
commonColumn = "naturalPersonFlag"

selectColumns = []

for colName in crmDF.columns:
    if colName == commonColumn:
        selectColumns.append(coalesce(col("isu." + colName), col("crm." + colName)).alias(colName))
    elif colName in crmOnlyFlagColumns:
        selectColumns.append(when(col("isu.businessPartnerNumber") == col("crm.businessPartnerNumber"), col("crm." + colName).alias(colName)) 
                                 .otherwise(lit('N')).alias(colName))
    elif colName in crmOnlyColumns:
        selectColumns.append(when(col("isu.businessPartnerNumber") == col("crm.businessPartnerNumber"), col("crm." + colName).alias(colName)) 
                                 .otherwise(lit(None)).alias(colName))
    else:
        selectColumns.append(when(col("isu.businessPartnerNumber").isNotNull(), col("isu." + colName).alias(colName))
                                .otherwise(col("crm." + colName)).alias(colName))


isuCrmdf = ((isuDF.alias("isu").join(
            crmDF.alias("crm"), 
       col("isu.businessPartnerNumber") ==  col("crm.businessPartnerNumber"), how='full')
     ).select(*selectColumns)).alias("mdf")
################################################################################################

# COMMAND ----------

#Aurion pick single record in priority (active/terminated/history) as of now
windowSpec = Window.partitionBy("businessPartnerNumber").orderBy("priority")
auriondf = aurDF.withColumn("selectInd", row_number().over(windowSpec))
auriondf = auriondf.filter(col("selectInd") == 1).drop("selectInd", "priority")
mainCols = set(isuCrmdf.columns)
aurCols  = set(auriondf.columns)

# if isSchemaChanged(currentDataFrame):
#     joinType = 'left'
# else:
#     joinType = 'full'
joinType = 'left'

###column Selects ##
finalSelect = [ when (col("mdf.businessPartnerNumber").isNotNull(),  
                     col(f"mdf.{col_name}")).otherwise(col(f"adf.{col_name}")).alias(col_name)
               if col_name in aurCols else col(f"mdf.{col_name}")  
               for col_name in isuCrmdf.columns     
             ]
finalSelect += [col(f"adf.{col_name}").alias(col_name) for col_name in aurCols if col_name not in mainCols]
##################

#now upsert maindf with aurion df
finaldf = ((isuCrmdf.alias("mdf").join(
      auriondf.alias("adf"), 
       col("mdf.businessPartnerNumber") ==  col("adf.businessPartnerNumber"), how= joinType)
     ).select(*finalSelect))

# COMMAND ----------

def Transform():
    global df    
    business_key = "businessPartnerNumber"
    df = finaldf
    
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
    if isSchemaChanged(df):        
        saveSchemaAndData(df, 'businessPartnerNumber', 'businessPartnerNumber')
        enableCDF(f"{getEnv()}cleansed.isu.0bpartner_attr")
        enableCDF(f"{getEnv()}cleansed.crm.0bpartner_attr")

    else:
        SaveWithCDF(df, 'SCD2') #Save(df) #SaveWithCDF(df, 'SCD2')
        #DisplaySelf()
Transform()
