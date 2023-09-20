# Databricks notebook source
# MAGIC %run ./common-constants

# COMMAND ----------

def ExecuteJDBCQuery(sql, targetJdbcKVSecretName):
  jdbc = GetJdbc(targetJdbcKVSecretName)
  connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc)
  connection.prepareCall(sql).execute()
  connection.close()

# COMMAND ----------

def RunQuery(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(GetJdbc("daf-sql-controldb-connectionstring"))
    return spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"({sql}) T")

# COMMAND ----------

def ExecuteStatement(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(GetJdbc("daf-sql-controldb-connectionstring"))
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc)
    connection.prepareCall(sql).execute()
    connection.close()

# COMMAND ----------

def WriteTable(sourceTable, targetTable, mode, jdbc):
    (spark.table(sourceTable)
    .write.mode(mode)
    .jdbc(jdbc, targetTable))

# COMMAND ----------

def GetJdbc(jdbcKVSecret):
  return dbutils.secrets.get(scope = SECRET_SCOPE, key = jdbcKVSecret)

# COMMAND ----------

def AppendTable(sourceTable, targetTable, targetJdbcKVSecretName):
  jdbc = GetJdbc(targetJdbcKVSecretName)
  WriteTable(sourceTable, targetTable, "append", jdbc)

# COMMAND ----------

def WriteSynapseTable(sourceDataFrame, targetTable, mode="overwrite"):
    accessKey = GetStorageKey()
    jdbc = GetSynapseJdbc()
    dataLakeName = GetDataLakeFqn().split(".")[0]
    blobFqn = f"{dataLakeName}.blob.core.windows.net"

    tempDir = f"wasbs://synapse@{blobFqn}/tempDirs"
    acntInfo = f"fs.azure.account.key.{blobFqn}"
    sc._jsc.hadoopConfiguration().set(acntInfo, accessKey)
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

    sourceDataFrame.write.format("com.databricks.spark.sqldw") \
    .option("url", jdbc) \
    .option("dbtable", targetTable) \
    .option("forward_spark_azure_storage_credentials", "True") \
    .option("tempdir", tempDir) \
    .mode(mode).save()

# COMMAND ----------

def CreateSqlTableFromSource(sourceTable, targetTable, targetJdbcKVSecretName):
    jdbc = GetJdbc(targetJdbcKVSecretName)
    sql = f"CREATE TABLE {targetTable} \
                USING org.apache.spark.sql.jdbc \
                OPTIONS ( \
                  url \"{jdbc}\", \
                  dbtable \"{sourceTable}\" \
                )"
    spark.sql(sql)

# COMMAND ----------

def GetSqlDataFrame(tableFqn, targetJdbcKVSecretName):
    jdbc = GetJdbc(targetJdbcKVSecretName)
    return spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=tableFqn)

# COMMAND ----------

def JdbcConnectionFromSqlConnectionString(connectionString):
    connectionList = dict()

    for i in connectionString.split(";"):
        if(len(i.split("=")) != 2): 
            continue

        key = i.split("=")[0].strip()
        value = i.split("=")[1].strip()
        connectionList[key] = value

    server = connectionList["Server"].replace(",1433", "").replace("tcp:", "")
    database = connectionList["Initial Catalog"] if connectionList.get("Initial Catalog") is not None else connectionList["Database"]
    username = connectionList["User ID"] if connectionList.get("User ID") is not None else connectionList["User Id"]
    password = connectionList["Password"]

    jdbc = f"jdbc:sqlserver://{server};DatabaseName={database};Persist Security Info=False;user={username};Password={password};"
    return jdbc
