-- Databricks notebook source
-- DBTITLE 1,Define Widgets (Parameters)
CREATE WIDGET TEXT env DEFAULT ""

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from Data Admin group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-DataAdmin`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-DataAdmin`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-DataAdmin`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-DataAdmin`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-DataAdmin`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to Data Admin group
GRANT ALL PRIVILEGES ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-DataAdmin`;
GRANT ALL PRIVILEGES ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-DataAdmin`;
GRANT ALL PRIVILEGES ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-DataAdmin`;
GRANT ALL PRIVILEGES ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-DataAdmin`;
GRANT ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-DataAdmin`;

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from Data Developer group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-DataDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-DataDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-DataDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-DataDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-DataDeveloper`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to Data Developer group
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-DataDeveloper`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-DataDeveloper`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-DataDeveloper`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-DataDeveloper`;
GRANT ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-DataDeveloper`;

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from Data Analyst Std User group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to Data Analyst Std User group
DENY ALL PRIVILEGES ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
DENY ALL PRIVILEGES ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
DENY ALL PRIVILEGES ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;
DENY ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-DataAnalystStdUsr`;

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from Data Analyst Adv User group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to Data Analyst Adv User group
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
DENY ALL PRIVILEGES ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;
GRANT ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-DataAnalystAdvUsr`;

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from BI Std User group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-BiStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-BiStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-BiStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-BiStdUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-BiStdUsr`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to BI Std User group
DENY ALL PRIVILEGES ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-BiStdUsr`;
DENY ALL PRIVILEGES ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-BiStdUsr`;
DENY ALL PRIVILEGES ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-BiStdUsr`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-BiStdUsr`;
DENY ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-BiStdUsr`;

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from BI Adv User group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-BiAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-BiAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-BiAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-BiAdvUsr`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-BiAdvUsr`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to BI Adv User group
DENY ALL PRIVILEGES ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-BiAdvUsr`;
DENY ALL PRIVILEGES ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-BiAdvUsr`;
DENY ALL PRIVILEGES ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-BiAdvUsr`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-BiAdvUsr`;
GRANT ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-BiAdvUsr`;

-- COMMAND ----------

-- DBTITLE 1,Revoke all ACLs from BI Developer User group
REVOKE ALL PRIVILEGES ON SCHEMA raw FROM `A-Azure-rg-$env-daf-01-BiDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA cleansed FROM `A-Azure-rg-$env-daf-01-BiDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA curated FROM `A-Azure-rg-$env-daf-01-BiDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA semantic FROM `A-Azure-rg-$env-daf-01-BiDeveloper`;
REVOKE ALL PRIVILEGES ON SCHEMA datalab FROM `A-Azure-rg-$env-daf-01-BiDeveloper`;

-- COMMAND ----------

-- DBTITLE 1,Grant all ACLs to BI Developer User group
DENY ALL PRIVILEGES ON SCHEMA raw TO `A-Azure-rg-$env-daf-01-BiDeveloper`;
DENY ALL PRIVILEGES ON SCHEMA cleansed TO `A-Azure-rg-$env-daf-01-BiDeveloper`;
DENY ALL PRIVILEGES ON SCHEMA curated TO `A-Azure-rg-$env-daf-01-BiDeveloper`;
GRANT USAGE, SELECT, READ_METADATA ON SCHEMA semantic TO `A-Azure-rg-$env-daf-01-BiDeveloper`;
GRANT ALL PRIVILEGES ON SCHEMA datalab TO `A-Azure-rg-$env-daf-01-BiDeveloper`;
