// Databricks notebook source
// DBTITLE 1,ReadMe
//#Need to add Maven library
//https://github.com/Azure/azure-sqldb-spark/releases/tag/1.0.2


// COMMAND ----------

// DBTITLE 1,Settings
var bulkCopyBatchSize = 50000

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import com.microsoft.azure.sqldb.spark.query._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
  

def getConfig(kvSecret:String, table:String) : Config = {
  var keyval = dbutils.secrets.get(scope="vwazr-dp-keyvault",key=kvSecret)
  var conf = Map[String,String]()
  var cons = keyval.split(';')
  for (con <- cons){
        val value = con.split('=')
        conf += (value(0) -> value(1))
  }
  
  val config = Config(Map(
      "url"            -> conf("data source").toLowerCase,
      "databaseName"   -> conf("initial catalog").toLowerCase,
      "dbTable"        -> table,
      "user"           -> conf("user id").toLowerCase,
      "password"       -> conf("password"),
      "connectTimeout" -> "30", //seconds
      "queryTimeout"   -> "600"  //seconds
    )
  )
  return config
}

def getConfigPusdown(kvSecret:String, query:String) : Config = {
  var keyval = dbutils.secrets.get(scope="vwazr-dp-keyvault",key=kvSecret)
  var conf = Map[String,String]()
  var cons = keyval.split(';')
  for (con <- cons){
        val value = con.split('=')
        conf += (value(0) -> value(1))
  }
  
  val config = Config(Map(
      "url"            -> conf("data source").toLowerCase,
      "databaseName"   -> conf("initial catalog").toLowerCase,
      "user"           -> conf("user id").toLowerCase,
      "password"       -> conf("password"),
      "queryCustom"    -> query
    )
  )
  return config
}

def readFromDBTable(kvSecret:String, table:String): DataFrame = {
  val config = getConfig(kvSecret, table)
  val df = spark.read.sqlDB(config)
  return df
}

def writeToDB(kvSecret: String, table:String, df: DataFrame, mode:String){
  val config = getConfig(kvSecret, table)
  if (mode.toLowerCase == "append"){
    df.write.mode(SaveMode.Append).sqlDB(config)
  }
  else if (mode.toLowerCase == "overwrite"){
    df.write.mode(SaveMode.Overwrite).sqlDB(config)
  }
}

def readQueryFromDB(kvSecret:String, query:String): DataFrame ={
  val config = getConfigPusdown(kvSecret, query)
  val df = spark.read.sqlDB(config)
  return df
}

def pushdownToDB(kvSecret:String, query:String)={
  val config = getConfigPusdown(kvSecret, query)
  spark.read.sqlDB(config)
}

// COMMAND ----------

// DBTITLE 1,Bulk Insert Functions


//Function to generate a cerate SQL table statement based on a dataframe
def getSQLCreateTableStatement(df: DataFrame, schemaName: String, tablename: String):String= {
  var statement:String = "create table [TMP].[" + schemaName + "_"  + tablename + "] ("
  var ColNull = "null"
  var i = 1
 
  for (col <- df.schema.fields) 
  {  
    if (col.nullable)
    {ColNull = "null"}
    else
    {ColNull = "not null"}
    
    //statement = statement + "\r\n" + col.name + " " + getSQLTypeScalaForCreateStatement(col.dataType.toString()) + " " + ColNull + ","
    statement = statement + "\r\n[" + col.name + "] " + getSQLTypeScalaForCreateStatement(col.dataType.toString()) + " " + ColNull + ","    
  }
  statement = statement.dropRight(1) + ");"  
  //print(statement)
  
  return statement
}


//Function to Map Types to Java SQL Types
def getSQLTypeScalaForBulkInsert(current_type: String):Int= {
  var rval = java.sql.Types.NVARCHAR
  if (current_type.toString() =="StringType")
  {rval = java.sql.Types.NVARCHAR}
  else if (current_type.toString() =="TimestampType")
  {rval = java.sql.Types.TIMESTAMP}
  else if (current_type.toString() =="DateType")
  {rval = java.sql.Types.TIMESTAMP}  
  else if (current_type.toString() =="IntegerType")
  {rval = java.sql.Types.INTEGER}
  else if (current_type.toString() =="LongType")
  {rval = java.sql.Types.BIGINT}  
  else if (current_type.toString() =="DoubleType")
  {rval = java.sql.Types.FLOAT}
  else if (current_type.toString() =="DecimalType")
  {rval = java.sql.Types.DECIMAL}
//   else if (current_type.toString() == "BinaryType")
//   {rval = java.sql.Types.BINARY}
  else
  {rval = java.sql.Types.NVARCHAR}
 rval
}


def getSQLTypeScalaForCreateStatement(current_type: String):String= {
  
  //print("CurrentType:" + current_type)  
  var rval = "NVARCHAR(MAX)"
  if (current_type.toString() =="StringType")
  {rval = "NVARCHAR(MAX)"}
  else if (current_type.toString() =="TimestampType")
  {rval ="DATETIME2"}
  else if (current_type.toString() =="DateType")
  {rval ="DATETIME2"}  
  else if (current_type.toString() =="IntegerType")
  {rval = "INT"}
  else if (current_type.toString() =="LongType")
  {rval = "BIGINT"}  
  else if (current_type.toString() =="DoubleType")
  {rval = "FLOAT"}
  else if (current_type.toString() =="DecimalType")
  {rval = "DECIMAL(18,8)"} 
//   else if (current_type.toString() == "BinaryType")
//   {rval = "VARBINARY(MAX)"}  
  else
  {rval = "NVARCHAR(MAX)"}
 rval
}


def bulkInsertSQLTempTable(kvSecret: String, df: DataFrame, schemaName: String, TempTableName: String):Long ={
/** 
  Add column Metadata.
  If not specified, metadata is automatically added
  from the destination table, which may suffer performance.
*/

  var TargetSQLTableName = schemaName+"."+TempTableName
  val config = getConfig(kvSecret, TargetSQLTableName)

  var bulkCopyMetadata = new BulkCopyMetadata
  var i = 1
  //var col
  for (col <- df.schema.fields) 
  {  
    bulkCopyMetadata.addColumnMetadata(i, col.name, getSQLTypeScalaForBulkInsert(col.dataType.toString()), 0, 0)
    
    //print(col.name + "," + getSQLTypeScalaForBulkInsert(col.dataType.toString()) + "," + col.dataType.toString() + "\n")
    
    i+=1
  }

  print("Created metadata\n")
  
  var bulkCopyConfig2 = Config(Map(
    "url"               -> config("url"),
    "databaseName"      -> config("databaseName"),
    "user"              -> config("user"),
    "password"          -> config("password"),
    //"databaseName"      -> "zeqisql",
    "dbTable"           -> TargetSQLTableName,
    "bulkCopyBatchSize" -> bulkCopyBatchSize.toString,
    //"bulkCopyTableLock" -> "true",    
    "bulkCopyTimeout"   -> "600"
  ))
  
  print("Created config, inserting\n")

  
  var starttime = System.nanoTime()
  //df.bulkCopyToSqlDB(bulkCopyConfig2, bulkCopyMetadata)
  df.bulkCopyToSqlDB(bulkCopyConfig2)
  var endtime = System.nanoTime()
  var elapsedsecs = (endtime-starttime)/1000000000.00
  
  print("finished\n")
  
  return 0
}



//execute a SQL command
def executeSQL(kvSecret: String, query: String){
  val config = getConfigPusdown(kvSecret, query)
  
  sqlContext.sqlDBQuery(config) match {
    case Left(s) => {}
    case Right(b) => {
      if (b == false){
        var d = 1/0        
      }
    }
  }
   

}


//An iterative bulk insert of data
//Add error handling!
def bulkInsertIterativeLoad(kvSecret: String, df: DataFrame, schemaName: String, tableName: String) {

  var TargetSQLTableName = tableName //dbutils.widgets.get("dstBLOBName")
  var dfCount = df.count()
  var iLimit = bulkCopyBatchSize*10
  //var noRuns = dfCount/iLimit
  var noRuns = math.ceil(dfCount.toDouble/iLimit.toDouble).toInt
  print("No of runs: " + noRuns)
  val arSize = Array.fill(noRuns.toInt)(1.0)
  var list_df = df.randomSplit(arSize) 
  
  print("List size is: " + list_df.length.toString + ", table rowcount: " + dfCount.toString + "\n")

  var i=0

  print("Iteration: "+ i.toString + ", Dropping table...\n")
  executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+TargetSQLTableName)

  //iterate through each array's DF to import the data!
  while (i < list_df.length) {

    print("Iteration: "+ i.toString + ", creating table...\n")
    var createQuery = getSQLCreateTableStatement(df, schemaName, TargetSQLTableName)
    executeSQL(kvSecret, createQuery)

    print("Iteration: "+ i.toString + ", bulk inserting table...\n")
    bulkInsertSQLTempTable(kvSecret, list_df(i), schemaName, TargetSQLTableName)  

//     try {
//       print("Iteration: "+ i.toString + ", Running Proc on table...\n")
//       val query = "EXEC ["+schemaName+"].[usp_"+tableName+"_Load] 0"
//       executeSQL(kvSecret, query)
//     }
//     catch {
//       v
      
//     }
//     finally{
//       print("Iteration: "+ i.toString + ", Dropping table...\n")
//       executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+TargetSQLTableName)
//     }

    print("Iteration: "+ i.toString + ", Running Proc on table...\n")
    val query = "EXEC ["+schemaName+"].[usp_"+tableName+"_Load] 0"
    executeSQL(kvSecret, query)

    print("Iteration: "+ i.toString + ", Dropping table...\n")
    executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+TargetSQLTableName)

    i += 1  
  }
  
}

//Full bulk insert load
def bulkInsertFullLoad(kvSecret: String, df: DataFrame, schemaName: String, tableName: String):Long = {

  print("*** Starting table: " + tableName + "\n")

  print("Truncate table data...\n")
  executeSQL(kvSecret, "TRUNCATE TABLE "+schemaName+"."+tableName)

  //print("Creating tempory table...\n")
  //var createQuery = getSQLCreateTableStatement(df, schemaName, tableName)
  //executeSQL(kvSecret, createQuery)

//   try {
//     print("Bulk inserting table...\n")
//     bulkInsertSQLTempTable(kvSecret, df, schemaName, tableName)  

//     print("Running Proc on table..." + "EXEC ["+schemaName+"].[usp_"+tableName+"_Load] 0\n")
//     val query = "EXEC ["+schemaName+"].[usp_"+tableName+"_Load] 0"
//     executeSQL(kvSecret, query)
//   }
//   catch {
//     case e: Throwable => println("Error: " + e)
//   }
//   finally {
//     print("Cleaning up temp table...\n")
//     executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+tableName)
//   }
  
  print("Bulk inserting table...\n")
  bulkInsertSQLTempTable(kvSecret, df, schemaName, tableName)  

  //print("Running Proc on table..." + "EXEC ["+schemaName+"].[usp_"+tableName+"_Load] 0\n")
  //val query = "EXEC ["+schemaName+"].[usp_"+tableName+"_Load] 0"
  //executeSQL(kvSecret, query)

  //print("Cleaning up temp table...\n")
  //executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+tableName)
  
  return 0
}



// COMMAND ----------

// DBTITLE 1,Dynamic Merge Loads
//Full bulk insert load
def bulkInsertFullLoad_DynamicMerge(kvSecret: String, df: DataFrame, schemaName: String, tableName: String):Long = {

  print("*** Starting table: " + tableName + "\n")
  print("Dropping table if exists...\n")
  executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+tableName)

  print("Creating tempory table...\n")
  var createQuery = getSQLCreateTableStatement(df, schemaName, tableName)
  executeSQL(kvSecret, createQuery)
 
  print("Bulk inserting table...\n")
  bulkInsertSQLTempTable(kvSecret, df, schemaName, tableName)  

  val query = "EXEC usp_MergeTMPTable @dstSchema = \'"+schemaName+"\', @dstTable = \'"+tableName+"\', @fullLoad = 0"
  print("Running Proc on table..." + query)
  executeSQL(kvSecret, query)

  print("Cleaning up temp table...\n")
  executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+tableName)
  
  return 0
}

//An iterative bulk insert of data
//Add error handling!
def bulkInsertIterativeLoad(kvSecret: String, df: DataFrame, schemaName: String, tableName: String) {

  var TargetSQLTableName = tableName //dbutils.widgets.get("dstBLOBName")
  var dfCount = df.count()
  var iLimit = bulkCopyBatchSize*10
  //var noRuns = dfCount/iLimit
  var noRuns = math.ceil(dfCount.toDouble/iLimit.toDouble).toInt
  print("No of runs: " + noRuns)
  val arSize = Array.fill(noRuns.toInt)(1.0)
  var list_df = df.randomSplit(arSize) 
  
  print("List size is: " + list_df.length.toString + ", table rowcount: " + dfCount.toString + "\n")

  var i=0

  print("Iteration: "+ i.toString + ", Dropping table...\n")
  executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+TargetSQLTableName)

  //iterate through each array's DF to import the data!
  while (i < list_df.length) {

    print("Iteration: "+ i.toString + ", creating table...\n")
    var createQuery = getSQLCreateTableStatement(df, schemaName, TargetSQLTableName)
    executeSQL(kvSecret, createQuery)

    print("Iteration: "+ i.toString + ", bulk inserting table...\n")
    bulkInsertSQLTempTable(kvSecret, list_df(i), schemaName, TargetSQLTableName)  

    val query = "EXEC usp_MergeTMPTable @dstSchema = \'"+schemaName+"\', @dstTable = \'"+tableName+"\', @fullLoad = 0"
    print("Running Proc on table..." + query)
    executeSQL(kvSecret, query)

    print("Iteration: "+ i.toString + ", Dropping table...\n")
    executeSQL(kvSecret, "DROP TABLE IF EXISTS TMP."+schemaName+"_"+TargetSQLTableName)

    i += 1  
  }
  
}
