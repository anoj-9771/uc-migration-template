# Databricks notebook source
# DBTITLE 1,Query to execute for Error: org.apache.spark.sql.AnalysisException: Cannot create the managed table('`testdb`.` testtable`'). The associated location ('dbfs:/user/hive/warehouse/testdb.db/metastore_cache_ testtable) already exists.; 
#run the below query incase the cluster is terminated while a write operation is in progress and you get above error. Below cmd will delete the _STARTED directory and returns the process to the original state. For example, you can set it in the notebook:
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
