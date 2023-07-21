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
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}")#.withColumn("_change_type", lit(None))
    derivedDF2 = GetTable(f"{getEnv()}{driverTable2}")#.withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, changeColumnsISU, True).filter(col("_change_type").rlike('update_postimage|insert'))
    derivedDF2 = getSourceCDF(driverTable2, changeColumnsCRM, True).filter(col("_change_type").rlike('update_postimage|insert'))
    if derivedDF1.count() == 0 and derivedDF2.count() == 0:
        print("No delta to be  processed")

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
        df.drop(col("_change_type"))
        saveSchemaAndData(df, 'businessPartnerNumber', 'businessPartnerNumber')
        enableCDF(f"{getEnv()}cleansed.isu.0bpartner_attr")
        enableCDF(f"{getEnv()}cleansed.crm.0bpartner_attr")

    else:
        Save(df) #SaveWithCDF(df, 'SCD2')
        #DisplaySelf()
Transform()
