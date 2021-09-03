// Databricks notebook source
// MAGIC %run ./global-variables-scala

// COMMAND ----------

def ScalaGetAzSqlDBConnectionURL(): String = {
  
  //This method gets the JDBC URL for Azure SQL
  //We need to pass the UserID and Password seperately using Connection Properties
  
  var JDBC_PORT =  "1433"
  val jdbcUrl = s"jdbc:sqlserver://${ADS_DB_SERVER}:${JDBC_PORT};database=${ADS_DATABASE_NAME}"
  
  return jdbcUrl
}

// COMMAND ----------

def ScalaGetAzSqlDBConnectionProperties(): java.util.Properties = {

  //This method builds the Connection Properties to be passed to connect to Azure SQL DB
  
  Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

  var DB_USER_NAME = ADS_DATABASE_USERNAME 
  var DB_PASSWORD = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)

  import java.util.Properties
  val connectionProperties = new Properties()

  connectionProperties.put("user", s"${DB_USER_NAME}")
  connectionProperties.put("password", s"${DB_PASSWORD}")

  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  connectionProperties.setProperty("Driver", driverClass)
  
  return connectionProperties
  
}

// COMMAND ----------

def ScalaGetDataFrameFromAzSqlDB(query:String): org.apache.spark.sql.DataFrame = {
  
  //Get the Connection JDBC URL
  var jdbcUrl = ScalaGetAzSqlDBConnectionURL()
  
  //Get the Connection Properties
  var connectionProperties = ScalaGetAzSqlDBConnectionProperties()
  
  //User the Query
  var pushdown_query = s"($query) qry"
  
  //Get a DataFrame from the Query
  val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
  
  return df
  
}

// COMMAND ----------

def ScalaExecuteSQLQuery(query: String, showRecordCount: Boolean = false) : String = {
  
  //This method allows runnning a T-SQL on a SQL Server
  //The advantage of the java based method is that it does not require installing library on the cluster and can also run on the latest Spark version

  import java.sql.Connection;
  import java.sql.ResultSet;
  import java.sql.Statement;
  import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

  //Retrieve the database password from Key Vault
  val dbpwd = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)

  val ds = new SQLServerDataSource();
  ds.setServerName(ADS_DB_SERVER); // Replace with your server name
  ds.setDatabaseName(ADS_DATABASE_NAME); // Replace with your database
  ds.setUser(ADS_DATABASE_USERNAME); // Replace with your user name
  ds.setPassword(dbpwd); // Replace with your password
  ds.setHostNameInCertificate("*.database.windows.net");

  val connection = ds.getConnection(); 

  try
  {
    println(query)
    val pStmt = connection.prepareStatement(query);
    val affectedRows = pStmt.executeUpdate();
    if (showRecordCount)
      println(affectedRows + " rows.")
  }
  catch
  {
    case unknown : Throwable  => throw new Exception(unknown)
  }
  
  return "1"
}


// COMMAND ----------

def ScalaCreateSchema (SchemaName:String): String= {
  
  //This method creates a schema on Azure SQL if if does not exists
  
  println("Creating Schema if it does not exists")
  //Build the SQL
  var SQL = s"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE NAME = '$SchemaName') \n"
  SQL += s"EXEC sp_executesql N'CREATE SCHEMA [$SchemaName]'"
  
  //Execute the SQL Query using our generic method
  ScalaExecuteSQLQuery(SQL)
  
  return "1"
}

// COMMAND ----------

// def ScalaExecuteSQLQuery(query: String) : String = {
//This method is being retired in favour of above method.
//The above method does not require installing the library and can also run on the latest Spark version
  
//   //This method allows runnning a T-SQL on a SQL Server

//   import com.microsoft.azure.sqldb.spark.config.Config
//   import com.microsoft.azure.sqldb.spark.query._

//   //Get the DB Password from the Key Vault
//   val dbpwd = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)
  
//   //Get the connection String and assign the T-SQL Query from the parameter
//   val config = Config(Map(
//     "url"          -> ADS_DB_SERVER,
//     "databaseName" -> ADS_DATABASE_NAME,
//     "user"         -> ADS_DATABASE_USERNAME,
//     "password"     -> dbpwd,
//     "queryCustom"  -> query,
//     "encrypt"      -> "true",
//     "trustServerCertificate"      -> "true"
//   ))
  
//   println(query)
  
//   //Execute the Query
//   sqlContext.sqlDBQuery(config)
  
//   return "1"
// }

// COMMAND ----------

def ScalaLoadDataToEDW(source_object:String, target_schema:String) : String = {

  //This is a generic method to load data to SQL Server EDW from trusted zone
  println(s"Starting : ScalaLoadDataToEDW : Load Delta Table $source_object to schema $target_schema")

  //Create the Schema at the beginning if it does not exists
  ScalaCreateSchema(target_schema)
  
  //Get the generated SQL Query from Python
  //The query was stored in a temporary table
  val sql_merge_table = source_object + "_sql_merge"
  val scalaDF = spark.table(sql_merge_table)
  val merge_sql_query = scalaDF.select("sql").collect()(0).toString().replace("[","").replace("]","")
  
  spark.sql("DROP TABLE IF EXISTS " + sql_merge_table)

  //Query to check if table exists
  val query_table_exists = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + target_schema + "' AND TABLE_NAME = '" + source_object + "'"
  
  //If EDW table does not exists in SQL Server, then simply transfer the schema of Stage table to EDW table
  //This should work as this is the first time
  val query_transfer_schema_object = "ALTER SCHEMA " + target_schema + " TRANSFER " + ADS_SQL_SCHEMA_STAGE + "." + source_object
  
  //If this is not the first time, then execute the SQL Merge syntax
  val query_load_table = "IF NOT EXISTS(" + query_table_exists + ") " + query_transfer_schema_object + "\n" + " ELSE " + "\n" + merge_sql_query
  
  //Execute the Query
  ScalaExecuteSQLQuery(query_load_table, true)
}

// COMMAND ----------

def ScalaTruncateStageTable(source_object:String) : String = {

  //This is a generic method to Truncate the Stage table on SQL Server
  
  val query_table_exists = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + ADS_SQL_SCHEMA_STAGE + "' AND TABLE_NAME = '" + source_object + "'"
  val query_truncate_table = "TRUNCATE TABLE " + ADS_SQL_SCHEMA_STAGE + "." + source_object
  val query_table_truncate_if_exist = "IF EXISTS(" + query_table_exists + ") " + query_truncate_table
  
  ScalaExecuteSQLQuery(query_table_truncate_if_exist)
}

// COMMAND ----------

def ScalaAlterTableDataTypeFromSchema(SchemaFileURL:String, SourceObject:String, TargetTable:String, DeltaColumn:String, DateCols:String) {
  import scala.io.Source
  import java.io.File

  var fileURL = SchemaFileURL
  println(fileURL)
  
  if (!scala.reflect.io.File(fileURL).exists) {
    println("Schema File does not exists")
    return
  }
  
  var TableName = TargetTable.split("\\.")(1)
  var SchemaName = TargetTable.split("\\.")(0)
  
  //Generate the SQL to get all the columns from SQLEDW 
  var sql = s"SELECT UPPER(column_name) column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '$SchemaName' AND TABLE_NAME = '$TableName'"
  println(sql)

  //Get the Query from SQLEDW in a dataframe
  var df = ScalaGetDataFrameFromAzSqlDB(sql)

  //Convert dataframe to a list so that it is easier to read them
  var lstTableMeta = df.select("column_name").collect().map(_(0)).toList

  var lstDateCols = DateCols.toUpperCase().split(",")

  var columnName, simpleColumnName, columnType, columnTypeUpdated, columnPrecision, columnScale = ""
  var columnSql = ""
  for (line <- Source.fromFile(fileURL).getLines) {
    var lineArray = line.split(",")
    columnName = "\""+lineArray(0)+"\""
    columnName = columnName.replace(' ', '_')
    simpleColumnName = columnName.replace("\"", "").toUpperCase() //This column will help during comparison. columnName contains " so does not match on compare
    columnType = lineArray(2).toLowerCase
    columnPrecision = lineArray(3)
    columnScale = lineArray(4)
    
    //Align data type for Non-SQL Server databases
    columnTypeUpdated = columnType match {
      case "varchar2" => "varchar"
      case "nvarchar2" => "nvarchar"
      case "number" => "numeric"
      case "blob" => "binary"
      case "tinyblob" => "binary"
      case "tinytext" => "varchar"
      case "mediumtext" => "varchar"
      case "longtext" => "varchar"
      case "longblob" => "varbinary"
      case "enum" => "varchar"
      case "set" => "varchar"
      case "bool" => "bit"
      case "boolean" => "bit"
      case "mediumint" => "int"
      case "integer" => "int"
      case "dec" => "decimal"
      case "year" => "date"
      case _ => columnType
    }
    
    var sql = columnTypeUpdated match {
      case "datetime2" | "datetimeoffset" | "time" | "binary" => (s"ALTER TABLE $TargetTable ALTER COLUMN $columnName $columnTypeUpdated($columnPrecision)")
      case "varchar" | "nvarchar" | "varbinary" | "nchar" | "char"=> {
        if (columnPrecision == "-1" | columnPrecision.toLong > 8000) {
          (s"ALTER TABLE $TargetTable ALTER COLUMN $columnName $columnTypeUpdated(max)")
        }
        else {
          (s"ALTER TABLE $TargetTable ALTER COLUMN $columnName $columnTypeUpdated($columnPrecision)")
        }
      }
      case "numeric" | "decimal" => {
        if (columnScale == "")  columnScale = "0" //Oracle : If no scale is specified, default scale is 0
        (s"ALTER TABLE $TargetTable ALTER COLUMN $columnName $columnTypeUpdated($columnPrecision, $columnScale)")
      }
      case "clob" | "nclob" => (s"ALTER TABLE $TargetTable ALTER COLUMN $columnName nvarchar(max)")
      case _ => (s"ALTER TABLE $TargetTable ALTER COLUMN $columnName $columnTypeUpdated")
    }
    //To be investigated as what is the maximum query length that this function can take
    //With a table with lots of columns, the query was trimming and alter column were not executed
    //Temporaray fix is to run each individual alter statement
    
    if (lstTableMeta.contains(simpleColumnName)){
      //If the column exists in the target table

      //But do not update the data type for our Param Columns
      //(MySQL stores the date columns as numbers and we have already converted these numbers to dates. The schema file says the column is number
      //And we do not want to change them back to the number
      if (!lstDateCols.contains(simpleColumnName)) {
        ScalaExecuteSQLQuery(sql)
        columnSql += sql + "\n"
      }
    }
    
  }
      
  //ScalaExecuteSQLQuery(columnSql)

  //The following block of code assigns the datetime2 datatype to all the system date columns
  //This will ensure that if there are any miliseconds in the datetime part it will be included for SQLEDW
  var DBricksTSColumnSQL = ""

  DBricksTSColumnSQL += (s"ALTER TABLE $TargetTable ALTER COLUMN $COL_RECORD_START datetime2(7) \n")
  DBricksTSColumnSQL += (s"ALTER TABLE $TargetTable ALTER COLUMN $COL_RECORD_END datetime2(7) \n")
  DBricksTSColumnSQL += (s"ALTER TABLE $TargetTable ALTER COLUMN $COL_DL_RAW_LOAD datetime2(7) \n")
  DBricksTSColumnSQL += (s"ALTER TABLE $TargetTable ALTER COLUMN $COL_DL_TRUSTED_LOAD datetime2(7) \n")

  //If there is _transaction_date in the target table then upate the data type for it
  if (lstTableMeta.contains(ADS_COLUMN_TRANSACTION_DT.toUpperCase()))
    DBricksTSColumnSQL += (s"ALTER TABLE $TargetTable ALTER COLUMN $ADS_COLUMN_TRANSACTION_DT datetime2(7) \n")

  ScalaExecuteSQLQuery (DBricksTSColumnSQL)
  
}

// COMMAND ----------

def ScalaLoadDataToAzSqlDB(dataframe:org.apache.spark.sql.DataFrame, schemaname:String, tablename:String, writemode:String) {
  
  println(s"Starting : ScalaLoadDataToAzSqlDB : Writing data to table $schemaname.$tablename with mode $writemode")
  
  //Get the Connection JDBC URL
  var jdbcUrl = ScalaGetAzSqlDBConnectionURL()
  
  //Get the Connection Properties
  var connectionProperties = ScalaGetAzSqlDBConnectionProperties()

  //Get the SQL Table Name
  val sqltablename = s"$schemaname.$tablename"
  
  //Write the dataframe to Azure SQL DB
  dataframe.write.mode(writemode).jdbc(jdbcUrl, sqltablename, connectionProperties)

  println(s"Finished : ScalaLoadDataToAzSqlDB : Writing data to table $schemaname.$tablename with mode $writemode")
}

// COMMAND ----------

def ScalaAzureTableExists(SchemaName:String, TableName:String): Boolean = {
  
  //This method checks if a Table exists on Azure SQL Server
  
  var query = s"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$SchemaName' AND TABLE_NAME = '$TableName'"
  
  //Get the Dataframe from SQL Query
  val df = ScalaGetDataFrameFromAzSqlDB(query)
  
  //Check if any record exists to determine if the table exists
  if (df.count() == 1) {
    return true
  }
  else {
    return false
  }
  
}

// COMMAND ----------

def ScalaLoadDeltaTableToAzureSQL(delta_table_name:String, schema_name:String, table_name:String): String= {
  
  //This method saves the dataframe to Azure SQL Database
  println(s"Starting : ScalaLoadDeltaTableToAzureSQL : Load Delta Table $delta_table_name to Azure $schema_name.$table_name")
  val df = spark.table(delta_table_name)

  //Create the Schema at the beginning if it does not exists
  ScalaCreateSchema(schema_name)
  
  //Check if the table exists
  if (ScalaAzureTableExists(schema_name, table_name))  {
    println("Truncating the Azure SQL table : " + table_name)
    //Truncate table if it exists
    var qry_truncate = s"TRUNCATE TABLE $schema_name.$table_name"
    ScalaExecuteSQLQuery(qry_truncate)
  }
  else {
    println(s"Creatig an empty table : $schema_name.$table_name")
    //If the table does not exists, then create a new table passing 0 records from dataframe with overwrite
    ScalaLoadDataToAzSqlDB(df.limit(0), schema_name, table_name, "overwrite")
  }
  
  println(s"Loading the table : $schema_name.$table_name")
  //Save the dataframe to Azure SQL DB using append as the table in the last step is either just created or truncated
  ScalaLoadDataToAzSqlDB(df, schema_name, table_name, "append")
  
  println(s"Finishing : ScalaLoadDeltaTableToAzureSQL : Load Delta Table $delta_table_name to Azure $schema_name.$table_name")
  return "1"
  
}