# Databricks notebook source
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

# COMMAND ----------

def RunQuery(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))
    print(jdbc)
    return spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"({sql}) T")

# COMMAND ----------

def ExecuteStatement(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc)
    connection.prepareCall(sql).execute()
    connection.close()

# COMMAND ----------

def WriteTable(sourceDf, targetTable, mode):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))
    df = sourceDf
    df.write \
    .mode(mode) \
    .jdbc(jdbc, targetTable)
