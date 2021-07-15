// Databricks notebook source
// MAGIC %run ../util-edw-tsql-scala

// COMMAND ----------

dbutils.widgets.removeAll()
//Define widgets at the beginning
dbutils.widgets.text("query", "", "SQL to Execute")

// COMMAND ----------

//Get the value of widgets in the Scala function
val query = dbutils.widgets.get("query")

// COMMAND ----------

//Scala Function to Execute TSQL
def ExecScalaQuery(query:String): String = {
  
  println("Running query " + query)
  //Call the standard function to execute the Query
  ScalaExecuteSQLQuery(query)
  
  return "1"
}
  

// COMMAND ----------

//Execute TSQL
ExecScalaQuery(query)
