// Databricks notebook source
// MAGIC %md ###NOTEBOOK ONLY HOPS THE SQL REQUEST TO ScalaExecuteSQLQuery SCALA NOTEBOOK

// COMMAND ----------

// MAGIC %run ./global-variables-scala

// COMMAND ----------

def ExecuteJDBCSQLQuery(query: String) : String = {
  import java.sql.Connection;
  import java.sql.ResultSet;
  import java.sql.Statement;
  import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

  val dbpwd = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)

  val ds = new SQLServerDataSource();
  ds.setServerName(ADS_DB_SERVER);
  ds.setDatabaseName(ADS_DATABASE_NAME);
  ds.setUser(ADS_DATABASE_USERNAME);
  ds.setPassword(dbpwd);
  ds.setHostNameInCertificate("*.database.windows.net");

  val connection = ds.getConnection(); 
  val pStmt = connection.prepareStatement(query);
  val affectedRows = pStmt.executeUpdate();
  return s"$affectedRows";
}

// COMMAND ----------

import scala.io.Source
var path = dbutils.widgets.get("queryFilePath")
var returnValue = ""

if(path.length > 0)
{
  val fileContents = Source.fromFile(path).getLines.mkString
  print(fileContents)
  try{
  returnValue = ExecuteJDBCSQLQuery(fileContents)
    } catch {
  case e: Exception => throw e;
  }
}

// COMMAND ----------

dbutils.notebook.exit("{\"affected_rows\":\""+returnValue+"\"}")

// COMMAND ----------

