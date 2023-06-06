# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

from pyspark.sql.functions import col,rank
from pyspark.sql.window import Window

# COMMAND ----------

# TEST QUERY

# COMMAND ----------

# %sql
# select count(1) from (
# Select  catree.categoryTreeID, catree.categoryTreeGUID, catree.validFromDatetime , catree.validToDatetime
# ,caL1.categoryID as CAT_ID_LEVEL1, ctL1.categoryDescription as CAT_DESC_LEVEL2,
# caL2.categoryID as CAT_ID_LEVEL2, ctL2.categoryDescription as CAT_DESC_LEVEL2,
# caL3.categoryID as CAT_ID_LEVEL3, ctL3.categoryDescription as CAT_DESC_LEVEL3,
# caL4.categoryID as CAT_ID_LEVEL4, ctL4.categoryDescription as CAT_DESC_LEVEL4
# from {get_table_namespace('cleansed', 'crm_crmc_erms_cat_as')} catree



# --LEVEL ONE
# LEFT JOIN  {get_table_namespace('cleansed', 'crm_crmc_erms_cat_hi')} hiL1 on catree.categoryTreeGUID = hiL1.categoryTreeGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ct')} parent on parent.categoryGUID = hiL1.parentGuid
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ca')} caL1 on hiL1.childGUID = caL1.categoryGUID and hiL1.categoryTreeGUID = caL1.categoryTreeGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ct')} ctL1 on caL1.categoryGUID = ctL1.categoryGUID



# --LEVEL TWO
# LEFT JOIN  {get_table_namespace('cleansed', 'crm_crmc_erms_cat_hi')} hiL2 on catree.categoryTreeGUID = hiL2.categoryTreeGUID AND hiL2.parentGuid = hil1.childGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ca')} caL2 on hiL2.childGUID = caL2.categoryGUID and hiL2.categoryTreeGUID = caL2.categoryTreeGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ct')} ctL2 on caL2.categoryGUID = ctL2.categoryGUID



# --LEVEL THREE
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_hi')} hiL3 on catree.categoryTreeGUID = hiL3.categoryTreeGUID AND hiL3.parentGuid = hil2.childGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ca')} caL3 on hiL3.childGUID = caL3.categoryGUID and hiL3.categoryTreeGUID = caL3.categoryTreeGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ct')} ctL3 on caL3.categoryGUID = ctL3.categoryGUID



# --LEVEL FOUR
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_hi')} hiL4 on catree.categoryTreeGUID = hiL4.categoryTreeGUID AND hiL4.parentGuid = hil3.childGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ca')} caL4 on hiL4.childGUID = caL4.categoryGUID and hiL4.categoryTreeGUID = caL4.categoryTreeGUID
# LEFT JOIN {get_table_namespace('cleansed', 'crm_crmc_erms_cat_ct')} ctL4 on caL4.categoryGUID = ctL3.categoryGUID



# -- # where
# -- # ----------------------------Testing only------------------------
# -- # catree.categoryTreeID = 'ZSW_SERV_REQ_RES_CAT'
# -- # AND caL3.categoryID = 'SW_138'
# -- # AND validFromDatetime = '2022-03-07T17:38:00.000+1100'

# -----------------------------------------------------------------
# --Check for LEVEL ONE Header
# AND parent.categoryDescription IS NULL
# )
# --------------------------------------

# COMMAND ----------

from pyspark.sql.functions import when, col

def Transform():
    global df
    # ------------- TABLES ----------------- #
    # SAP application has option to choose hierarchy upto 4 levels
# LEVEL 1
    main_df = GetTable(f"{SOURCE}.crm_crmv_erms_cat_ca").select("categoryTreeID","categoryTreeDescription","categoryTreeGUID").dropDuplicates()
    category_df = GetTable(f"{SOURCE}.crm_crmd_srv_subject").select(col("categoryTreeID").alias("subjectCatogoryTreeId"),"catalogType").dropna().dropDuplicates()
    time_slice_df = GetTable(f"{SOURCE}.crm_crmc_erms_cat_as").select("categoryTreeGUID","categoryTreeID","validFromDatetime","validToDatetime").dropDuplicates()
#     time_slice_df = time_slice_df.withColumn("currentRecord",when(time_slice_df.validToDatetime > parser.parse('9999-12-31'), lit(1)).otherwise(lit(0)))
    hierarchy_df = GetTable(f"{SOURCE}.crm_crmc_erms_cat_hi").select("categoryTreeGUID","parentGuid","childGuid","nodeLeafFlag").dropDuplicates()
    category_level_df = GetTable(f"{SOURCE}.crm_crmc_erms_cat_ca").select("categoryGUID","categoryId","categoryTreeGUID").dropDuplicates()
    category_level_desc_df = GetTable(f"{SOURCE}.crm_crmc_erms_cat_ct").select("categoryGUID","categoryDescription").dropDuplicates()
    
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
   

    # ------------- JOINS ------------------ #
    windowSpec  = Window.partitionBy("business_key")
    windowSpecSourceBusinessKey  = Window.partitionBy("sourceBusinessKey")
    
    
    l1_join_df = main_df.join(time_slice_df,["categoryTreeGUID","categoryTreeID"],"inner") \
    .join(category_df,main_df.categoryTreeID==category_df.subjectCatogoryTreeId,"left") \
    .join(hierarchy_df,"categoryTreeGUID","inner") \
    .join(category_level_df.withColumnRenamed("categoryGUID","childGuid"),["childGuid","categoryTreeGUID"],"left").withColumnRenamed("categoryId","categoryLevel1Code") \
    .join(category_level_desc_df,hierarchy_df.parentGuid == category_level_desc_df.categoryGUID,"left").withColumnRenamed("categoryDescription","categoryDescription_null") \
    .join(category_level_desc_df.withColumnRenamed("categoryGUID","childGuid"),["childGuid"],"left").withColumnRenamed("categoryDescription","categoryLevel1Description") \
    .filter("categoryDescription_null is null") 
    
    df1 = l1_join_df \
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel1Code", "validFromDatetime")) \
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel1Code")) \
    .withColumn("categoryLevel2Code", lit(None)) \
    .withColumn("categoryLevel2Description",  lit(None)) \
    .withColumn("categoryLevel3Code",  lit(None)) \
    .withColumn("categoryLevel3Description",  lit(None)) \
    .withColumn("categoryLevel4Code",  lit(None)) \
    .withColumn("categoryLevel4Description",  lit(None)) 
    
   
    df1 = df1.withColumn("rank",rank().over(windowSpec.orderBy(col("nodeLeafFlag").desc()))) \
        .filter("rank == 1").drop("rank")
    
    l2_join_df = l1_join_df \
    .join(h2,(hierarchy_df.childGuid == h2.L2parentGuid) & (main_df.categoryTreeGUID == h2.L2categoryTreeGUID) ,"left") \
    .join(catL2,["L2categoryTreeGUID","L2childGuid"],"left") \
    .join(cat_DescL2,h2.L2childGuid == cat_DescL2.L2categoryGUID ,"left") 
    
    
    df2 = l2_join_df \
    .filter("categoryLevel2Code is not null") \
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel2Code", "validFromDatetime")) \
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel2Code")) \
    .withColumn("categoryLevel3Code",  lit(None)) \
    .withColumn("categoryLevel3Description",  lit(None)) \
    .withColumn("categoryLevel4Code",  lit(None)) \
    .withColumn("categoryLevel4Description",  lit(None))
    
    df2 = df2.withColumn("rank",rank().over(windowSpec.orderBy(col("L2nodeLeafFlag").desc()))) \
    .filter("rank == 1").drop("rank")
    
    l3_join_df = l2_join_df \
    .join(h3,(h2.L2childGuid == h3.L3parentGuid) & (main_df.categoryTreeGUID == h3.L3categoryTreeGUID) ,"left") \
    .join(catL3,["L3categoryTreeGUID","L3childGuid"],"left") \
    .join(cat_DescL3,h3.L3childGuid == cat_DescL3.L3categoryGUID ,"left") 
    
    df3 = l3_join_df \
    .filter("categoryLevel3Code is not null") \
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel3Code", "validFromDatetime")) \
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel3Code")) \
    .withColumn("categoryLevel4Code",  lit(None)) \
    .withColumn("categoryLevel4Description",  lit(None))
    
    df3 = df3.withColumn("rank",rank().over(windowSpec.orderBy(col("L3nodeLeafFlag").desc()))) \
    .filter("rank == 1").drop("rank")
        
    df4 = l3_join_df \
    .join(h4,(h3.L3childGuid == h4.L4parentGuid) & (main_df.categoryTreeGUID == h4.L4categoryTreeGUID) ,"left") \
    .join(catL4,["L4categoryTreeGUID","L4childGuid"],"left") \
    .join(cat_DescL4,h4.L4childGuid == cat_DescL4.L4categoryGUID ,"left") \
    .filter("categoryLevel4Code is not null") \
    .withColumn("business_key", concat_ws('|', "categoryTreeID", "categoryLevel4Code", "validFromDatetime")) \
    .withColumn("sourceBusinessKey", concat_ws('|', "categoryTreeID", "categoryLevel4Code")) \
    
    columns =["business_key","catalogType","categoryTreeID","subjectCatogoryTreeId","categoryTreeDescription","categoryLevel1Code","categoryLevel1Description","categoryLevel2Code","categoryLevel2Description","categoryLevel3Code","categoryLevel3Description","categoryLevel4Code",
              "categoryLevel4Description","validFromDatetime","validToDatetime", "sourceBusinessKey"]
    
    df = df1.select([col for col in columns]).union(df2.select([col for col in columns])).union(df3.select([col for col in columns])).union(df4.select([col for col in columns])).distinct()
        
    # ------------- TRANSFORMS ------------- #
    
    df = df.withColumn("categoryUsage", when( df.subjectCatogoryTreeId.isNull(), lit("Interaction")).otherwise(lit("Service Request"))) \
    .withColumn("categoryType", when(df.catalogType== 'D', lit("Received Category")).otherwise(lit("Resolution Category"))) \
    .withColumn("sourceRecordCurrent", when(year(col("validToDatetime")) == '9999', 1).otherwise(0)) \
    .withColumn("sourceValidToDatetime",lag("ValidFromDatetime",1).over( windowSpecSourceBusinessKey.orderBy(col("ValidFromDatetime").desc())))
     
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
        ,"CASE WHEN YEAR(validToDatetime) = 9999 THEN validToDatetime ELSE dateadd(millisecond,-1,sourceValidToDatetime) END sourceValidToDatetime"
        ,"sourceRecordCurrent       sourceRecordCurrent"
        ,"sourceBusinessKey         sourceBusinessKey"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


