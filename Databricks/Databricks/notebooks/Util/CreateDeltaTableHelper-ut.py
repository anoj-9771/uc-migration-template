# Databricks notebook source
# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
# MAGIC set spark.databricks.optimizer.dynamicPartitionPruning = true;
# MAGIC set spark.databricks.io.cache.enabled = true;
# MAGIC set spark.sql.cbo.enabled = false;
# MAGIC -- # set spark.sql.autoBroadcastJoinThreshold=-1;
# MAGIC -- # set spark.sql.files.maxPartitionBytes = 134217728;
# MAGIC -- # set spark.sql.shuffle.partitions = 200;
# MAGIC -- # --set spark.default.parallelism = 200;
# MAGIC -- # set spark.sql.inMemoryColumnarStorage.compressed = true;
# MAGIC -- # set spark.sql.inMemoryColumnarStorage.batchSize = 10000;
# MAGIC -- # set spark.sql.caseSensitive = false;

# COMMAND ----------

# Update table to SCD Type 2
def buildDeltaTable(df_, watermark):
    # Create window function for assigning a row number for 
    # composite key partitions orderd by watermark filed
        
    
    # Create dataframe with added columns filtered by the most 
    # recent version of an item based on the applied window.
    if watermark: 
        window = Window.partitionBy("BK_DeltaLake").orderBy(desc(watermark))
        return (df_.select(cols_)
          .withColumn("HASH_CODE_DeltaLake", sha2(concat_ws("||", *check_sum), 256))
          .withColumn("LOAD_ID_DeltaLake", lit((datetime.today() + timedelta(hours=8))))
          .withColumn("IS_CURRENT_DeltaLake", lit(True))
          .withColumn("VALID_FROM_DeltaLake", lit((datetime.today() + timedelta(hours=8))))
          .withColumn("VALID_TO_DeltaLake", lit(datetime.strptime("9999-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")))
          .withColumn("BK_DeltaLake", sha2(concat_ws("||", *keys), 256))
          .withColumn('row_number', row_number().over(window))
          .filter('BK_DeltaLake is not null AND row_number = 1'))
    else:
        window = Window.partitionBy("BK_DeltaLake").orderBy("BK_DeltaLake")
        return (df_.select(cols_)
          .withColumn("HASH_CODE_DeltaLake", sha2(concat_ws("||", *check_sum), 256))
          .withColumn("LOAD_ID_DeltaLake", lit((datetime.today() + timedelta(hours=8))))
          .withColumn("IS_CURRENT_DeltaLake", lit(True))
          .withColumn("VALID_FROM_DeltaLake", lit((datetime.today() + timedelta(hours=8))))
          .withColumn("VALID_TO_DeltaLake", lit(datetime.strptime("9999-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")))
          .withColumn("BK_DeltaLake", sha2(concat_ws("||", *keys), 256))
          .withColumn('row_number', row_number().over(window))
          .filter('BK_DeltaLake is not null AND row_number = 1'))
    
# Incremental Delta table without SCD T2 feature enabled (Inserts and Updates)    
def perform_upsert(df_, watermark):    
    # Get new records
    newRecords = (df_.select(cols_delta)
      .alias("updates")
      .join(deltaTable.toDF().select(cols_delta).alias("target"), "BK_DeltaLake")
      .where("target.IS_CURRENT_DeltaLake = 1 AND target.HASH_CODE_DeltaLake <> updates.HASH_CODE_DeltaLake")
      .select("updates.*")) 
    # Get staged updates
    stagedUpdates = (
      newRecords.select(cols_delta)
      .selectExpr("NULL as mergeKey", "updates.*")
      .union(
        df_.select(cols_delta).selectExpr("BK_DeltaLake as mergeKey", "*")
      )
    )
    # Perform merge
    (deltaTable.alias("target")
    .merge(
        stagedUpdates.alias("staged_updates"),
        "target.BK_DeltaLake = mergeKey")
    .whenMatchedUpdate(
      condition = "target.IS_CURRENT_DeltaLake = 1 AND target.HASH_CODE_DeltaLake <> staged_updates.HASH_CODE_DeltaLake",
      set = {
              "VALID_TO_DeltaLake": "staged_updates.VALID_FROM_DeltaLake", 
              "IS_CURRENT_DeltaLake": "0"
            })
    .whenNotMatchedInsertAll()
    .execute()) 
    # Perform delete
    if not watermark:  
        # Get records to be deleted
        deletedRecords = (deltaTable.toDF()
                  .where(deltaTable.toDF().IS_CURRENT_DeltaLake == 1)
                  .select(cols_delta)
                  .alias("deletes")
                  .join(df_.select(cols_delta).alias("source"), "BK_DeltaLake", how = "left_anti")
                  .select("deletes.*"))
        stagedDel = (
                  deletedRecords.select(cols_delta)
                  .selectExpr("NULL as mergeKey", "deletes.*")
                )
        # Perform deletes
        (deltaTable.alias("target")
        .merge(
            stagedDel.alias("staged_deletes"),
            "target.BK_DeltaLake = staged_deletes.BK_DeltaLake")
        .whenMatchedUpdate(
          condition = "target.IS_CURRENT_DeltaLake = 1 AND staged_deletes.mergeKey is null",
          set = {
                  "VALID_TO_DeltaLake": "current_timestamp()", 
                  "IS_CURRENT_DeltaLake": "0"
                })
        .execute()) 
