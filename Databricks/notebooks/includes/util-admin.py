# Databricks notebook source
def AdminRemoveDeltaTable(fq_table_name):
  #The method removes the delta table and its associated files from Data Lake
  #PLEASE BE EXTREMELY CAREFUL BEFORE RUNNING THIS
  #ONCE THE DATA IS GONE, IT CANNOT BE RECOVERED
  #AdminRemoveDeltaTable("taleo_requisition_master", "raw")
  query = f"DESCRIBE DETAIL {fq_table_name}"
  df = spark.sql(query)
  
  file_url = df.select("location").rdd.flatMap(list).collect()[0]
  
  print(f"Dropping Table {fq_table_name}")
  spark.sql(f"DROP TABLE IF EXISTS {fq_table_name}")
  
  print("Clearning Data Lake Files")
  dbutils.fs.rm(file_url, True)
  
  print ("ALL DONE!!")
  
