# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		col1 STRING NOT NULL,
		col2 STRING NOT NULL,
		col3 STRING  NOT NULL ,
		col4 DATE  NOT NULL ,
		col5 INTEGER  NOT NULL ,
		col6 STRING  NOT NULL ,
		col7 STRING  NOT NULL ,
		col8 DECIMAL(18,10),
	)
"""
)

# COMMAND ----------

df = spark.sql(f"""
      SELECT *
			FROM {get_env()cleansed}.source.table1 t1
			JOIN {get_env()cleansed}.source.table2 t2
		"""
)

#write out the data to the table
save_df(df, append=True)