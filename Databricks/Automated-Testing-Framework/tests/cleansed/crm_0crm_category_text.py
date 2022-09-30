# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC 1. Need to ensures schema is matching with the new cleansed mapping (or)
# MAGIC    Need to ensure the table data is populated as expected while doing a 1:1 and transformation check 
# MAGIC    - lookup tag 
# MAGIC    - transformation 
# MAGIC    - custom transform tag 
# MAGIC    
# MAGIC 2.can be done by performing using separate defined function 
# MAGIC  need the mapping csv location or file 
# MAGIC 3. repeat all the other scenarios 
# MAGIC  - count 
# MAGIC  - duplicate 
# MAGIC  - business keys NULL and blank
# MAGIC  - code and desc field 
# MAGIC  - audit columns are populated as per the tech rec
# MAGIC  - need to check on the scenario which Miko mentioned for delta 
# MAGIC  - date validation 
# MAGIC  - sample check to see if the data populated is as per the expected scenario
# MAGIC  */

# COMMAND ----------

tgt=spark.sql("Select * from cleansed.crm_crmd_partner") # cleansed.crm_0crm_category_text
tgt.printSchema()
display(tgt)

# COMMAND ----------

a=spark.read.csv("dbfs:/mnt/datalake-raw/cleansed_csv/devmaximo_cleansed.csv")
display(a)

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
