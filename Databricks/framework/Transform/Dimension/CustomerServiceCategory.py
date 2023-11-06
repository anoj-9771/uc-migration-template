# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

from pyspark.sql.functions import col,rank,when, col
from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import col,rank
from pyspark.sql.window import Window

# COMMAND ----------

###cleansed layer table (cleansed.crm.crmv_erms_cat_ca/cleansed.crm.crmc_erms_cat_as/cleansed.crm.crmc_erms_cat_hi/cleansed.crm.crmc_erms_cat_ca/crm.crmc_erms_cat_ct) is full reload ###
###cleansed layer table cleansed.crm.crmd_srv_subject has delta reload ##
## Full reload of table no CDF used ## as not enought table provides delta ####
main_df = GetTable(f"{getEnv()}cleansed.crm.crmv_erms_cat_ca").select("categoryTreeID","categoryTreeDescription","categoryTreeGUID").dropDuplicates()
category_df = GetTable(f"{getEnv()}cleansed.crm.crmd_srv_subject").select(col("categoryTreeID").alias("subjectCatogoryTreeId"),"catalogType").dropna().dropDuplicates()
time_slice_df = GetTable(f"{getEnv()}cleansed.crm.crmc_erms_cat_as").select("categoryTreeGUID","categoryTreeID","validFromDatetime","validToDatetime").dropDuplicates()

hierarchy_df = GetTable(f"{getEnv()}cleansed.crm.crmc_erms_cat_hi").select("categoryTreeGUID","parentGuid","childGuid","nodeLeafFlag").dropDuplicates()
category_level_df = GetTable(f"{getEnv()}cleansed.crm.crmc_erms_cat_ca").select("categoryGUID","categoryId","categoryTreeGUID").dropDuplicates()
category_level_desc_df = GetTable(f"{getEnv()}cleansed.crm.crmc_erms_cat_ct").select("categoryGUID","categoryDescription").dropDuplicates()
    
# LEVEL 2 
h2=hierarchy_df.select(col("categoryTreeGUID").alias("L2categoryTreeGUID"),col("parentGuid").alias("L2parentGuid"),col("childGuid").alias("L2childGuid"),col("nodeLeafFlag").alias("L2nodeLeafFlag"))
catL2=category_level_df.select(col("categoryTreeGUID").alias("L2categoryTreeGUID"),col("categoryId").alias("categoryLevel2Code"),col("categoryGUID").alias("L2childGuid"))
cat_DescL2 = category_level_desc_df.select(col("categoryGUID").alias("L2categoryGUID"),col("categoryDescription").alias("categoryLevel2Description"))

# LEVEL3
h3=hierarchy_df.select(col("categoryTreeGUID").alias("L3categoryTreeGUID"),col("parentGuid").alias("L3parentGuid"),col("childGuid").alias("L3childGuid"),col("nodeLeafFlag").alias("L3nodeLeafFlag"))
catL3=category_level_df.select(col("categoryTreeGUID").alias("L3categoryTreeGUID"),col("categoryId").alias("categoryLevel3Code"),col("categoryGUID").alias("L3childGuid"))
cat_DescL3 = category_level_desc_df.select(col("categoryGUID").alias("L3categoryGUID"),col("categoryDescription").alias("categoryLevel3Description"))

# LEVEL 4
h4=hierarchy_df.select(col("categoryTreeGUID").alias("L4categoryTreeGUID"),col("parentGuid").alias("L4parentGuid"),col("childGuid").alias("L4childGuid"))
catL4=category_level_df.select(col("categoryTreeGUID").alias("L4categoryTreeGUID"),col("categoryId").alias("categoryLevel4Code"),col("categoryGUID").alias("L4childGuid"))
cat_DescL4 = category_level_desc_df.select(col("categoryGUID").alias("L4categoryGUID"),col("categoryDescription").alias("categoryLevel4Description"))


# COMMAND ----------

# ------------- JOINS ------------------ #
windowSpec  = Window.partitionBy("business_key")
windowSpecSourceBusinessKey  = Window.partitionBy("sourceBusinessKey")
    
    
l1_join_df = (main_df.join(time_slice_df,["categoryTreeGUID","categoryTreeID"],"inner") 
    .join(category_df,main_df.categoryTreeID==category_df.subjectCatogoryTreeId,"left") 
    .join(hierarchy_df,"categoryTreeGUID","inner") 
    .join(category_level_df.withColumnRenamed("categoryGUID","childGuid"),["childGuid","categoryTreeGUID"],"left").withColumnRenamed("categoryId","categoryLevel1Code") 
    .join(category_level_desc_df,hierarchy_df.parentGuid == category_level_desc_df.categoryGUID,"left").withColumnRenamed("categoryDescription","categoryDescription_null") 
    .join(category_level_desc_df.withColumnRenamed("categoryGUID","childGuid"),["childGuid"],"left").withColumnRenamed("categoryDescription","categoryLevel1Description") 
    .filter("categoryDescription_null is null"))
    
df1 = (l1_join_df 
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel1Code", "validFromDatetime")) 
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel1Code")) 
    .withColumn("categoryLevel2Code", lit(None)) 
    .withColumn("categoryLevel2Description",  lit(None)) 
    .withColumn("categoryLevel3Code",  lit(None)) 
    .withColumn("categoryLevel3Description",  lit(None)) 
    .withColumn("categoryLevel4Code",  lit(None)) 
    .withColumn("categoryLevel4Description",  lit(None)))
    
   
df1 = (df1.withColumn("rank",rank().over(windowSpec.orderBy(col("nodeLeafFlag").desc()))) 
        .filter("rank == 1").drop("rank"))
    
l2_join_df = (l1_join_df 
    .join(h2,(hierarchy_df.childGuid == h2.L2parentGuid) & (main_df.categoryTreeGUID == h2.L2categoryTreeGUID) ,"left") 
    .join(catL2,["L2categoryTreeGUID","L2childGuid"],"left") 
    .join(cat_DescL2,h2.L2childGuid == cat_DescL2.L2categoryGUID ,"left"))
    
    
df2 = (l2_join_df 
    .filter("categoryLevel2Code is not null") 
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel2Code", "validFromDatetime")) 
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel2Code")) 
    .withColumn("categoryLevel3Code",  lit(None)) 
    .withColumn("categoryLevel3Description",  lit(None)) 
    .withColumn("categoryLevel4Code",  lit(None)) 
    .withColumn("categoryLevel4Description",  lit(None)))
    
df2 = (df2.withColumn("rank",rank().over(windowSpec.orderBy(col("L2nodeLeafFlag").desc()))) 
    .filter("rank == 1").drop("rank"))
    
l3_join_df = (l2_join_df 
    .join(h3,(h2.L2childGuid == h3.L3parentGuid) & (main_df.categoryTreeGUID == h3.L3categoryTreeGUID) ,"left") 
    .join(catL3,["L3categoryTreeGUID","L3childGuid"],"left") 
    .join(cat_DescL3,h3.L3childGuid == cat_DescL3.L3categoryGUID ,"left"))
    
df3 = (l3_join_df 
    .filter("categoryLevel3Code is not null") 
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel3Code", "validFromDatetime")) 
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel3Code")) 
    .withColumn("categoryLevel4Code",  lit(None)) 
    .withColumn("categoryLevel4Description",  lit(None)))
    
df3 = (df3.withColumn("rank",rank().over(windowSpec.orderBy(col("L3nodeLeafFlag").desc()))) 
    .filter("rank == 1").drop("rank"))
        
df4 = (l3_join_df 
    .join(h4,(h3.L3childGuid == h4.L4parentGuid) & (main_df.categoryTreeGUID == h4.L4categoryTreeGUID) ,"left") 
    .join(catL4,["L4categoryTreeGUID","L4childGuid"],"left") 
    .join(cat_DescL4,h4.L4childGuid == cat_DescL4.L4categoryGUID ,"left") 
    .filter("categoryLevel4Code is not null") 
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel4Code", "validFromDatetime")) 
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel4Code")))
    
columns =["business_key","catalogType","categoryTreeID","subjectCatogoryTreeId","categoryTreeDescription","categoryLevel1Code","categoryLevel1Description","categoryLevel2Code",
          "categoryLevel2Description","categoryLevel3Code","categoryLevel3Description","categoryLevel4Code","categoryLevel4Description","validFromDatetime",
          "validToDatetime", "sourceBusinessKey"]
    
df = (df1.select([col for col in columns])
        .union(df2.select([col for col in columns]))
        .union(df3.select([col for col in columns]))
        .union(df4.select([col for col in columns])).distinct())
        

    
df = (df.withColumn("categoryUsage", when( df.subjectCatogoryTreeId.isNull(), lit("Interaction")).otherwise(lit("Service Request"))) 
       .withColumn("categoryType", when(df.catalogType== 'D', lit("Received Category")).otherwise(lit("Resolution Category"))) 
       .withColumn("sourceRecordCurrent", when(year(col("validToDatetime")) == '9999', 1).otherwise(0)) 
       .withColumn("sourceValidToDatetime",lag("ValidFromDatetime",1).over( windowSpecSourceBusinessKey.orderBy(col("ValidFromDatetime").desc()))))
     
windowSpec = Window.partitionBy("sourceBusinessKey").orderBy("validFromDatetime")
df = df.withColumn("rownum", row_number().over(windowSpec))
df = df.withColumn("validFromDatetime", when(col("rownum") == 1, to_timestamp(lit("1900-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss")).otherwise(col("validFromDatetime")))

if not(TableExists(_.Destination)):
    df = df.unionByName(spark.createDataFrame([dummyRecord(df.schema)], df.schema))  

# COMMAND ----------

def Transform():
    global finaldf
    finaldf = df
    _.Transforms = [
        f"business_key {BK}"
        ,"categoryUsage             customerServiceCategoryUsageName"
        ,"categoryType              customerServiceCategoryTypeName"
        ,"categoryTreeID            customerServiceCategoryGroupCode"
        ,"categoryTreeDescription   customerServiceCategoryGroupDescription"
        ,"categoryLevel1Code        customerServiceCategoryLevel1Code"
        ,"categoryLevel1Description customerServiceCategoryLevel1Description"
        ,"categoryLevel2Code        customerServiceCategoryLevel2Code"
        ,"categoryLevel2Description customerServiceCategoryLevel2Description"
        ,"categoryLevel3Code        customerServiceCategoryLevel3Code"
        ,"categoryLevel3Description customerServiceCategoryLevel3Description"
        ,"categoryLevel4Code        customerServiceCategoryLevel4Code"
        ,"categoryLevel4Description customerServiceCategoryLevel4Description"
        ,"validFromDatetime         sourceValidFromDatetime"
        ,"CASE WHEN sourceValidToDatetime IS NULL THEN TO_DATE('9999-12-31 23:59:59') WHEN YEAR(validToDatetime) = 9999 THEN validToDatetime ELSE dateadd(millisecond,-1,sourceValidToDatetime) END sourceValidToDatetime"
        ,"CASE WHEN sourceValidToDatetime IS NULL THEN '1' ELSE sourceRecordCurrent END sourceRecordCurrent"
        ,"sourceBusinessKey         sourceBusinessKey"
    ]
    finaldf = finaldf.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(finaldf)
    #SaveWithCDF(finaldf, 'Full Load')
    #DisplaySelf()
pass
Transform()
