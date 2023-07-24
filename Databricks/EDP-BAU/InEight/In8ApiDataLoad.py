# Databricks notebook source
#Import required liabararies
import requests
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /MDP-Framework/Common/common-helpers

# COMMAND ----------

spark.sql(f"""create schema if not exists {get_env()}raw.ineight""")

# COMMAND ----------

spark.sql(f"""create schema if not exists {get_env()}cleansed.ineight""")

# COMMAND ----------

#Global Variables
#This will be replaced with widgets during deployment
varuserid='SWADMIN'
varcompanyid='SW'
varpassword=dbutils.secrets.get('ADS','daf-ineight-api-pwd')
varapplication='APIDemo'
varreportnos=["01.030","04.001","07.010"]
#Report 07.010 Get Issues and can not load the data
# varreportnos=["01.030","04.001"]


# COMMAND ----------

#Schmea of project list
dfschemaprojectlist = StructType([ \
    StructField("Int_Proj",StringType(),True), \
    StructField("MailNewCount",StringType(),True), \
    StructField("DocsForReview",StringType(),True), \
    StructField("DocsForRelease",StringType(),True), \
    StructField("MailUnregisteredCount",StringType(),True), \
    StructField("OutstMLCnt",StringType(),True), \
    StructField("ProjNo",StringType(),True), \
    StructField("ProjTitle",StringType(),True), \
    StructField("ServerUrl",StringType(),True), \
    StructField("Int_Server",StringType(),True), \
    StructField("Int_Key",StringType(),True), \
    ])

# COMMAND ----------

#Schmea of report 01.030
dfschema01030 = ArrayType(
elementType =StructType([ \
    StructField("Int_Key",StringType(),True), \
    StructField("GroupTitle",StringType(),True), \
    StructField("Id",StringType(),True), \
    StructField("FirstName",StringType(),True), \
    StructField("LastName",StringType(),True), \
    StructField("Address1",StringType(),True), \
    StructField("Address2",StringType(),True), \
    StructField("Title",StringType(),True), \
    StructField("PostalAdr",StringType(),True), \
    StructField("City",StringType(),True), \
    StructField("State",StringType(),True), \
    StructField("PostCode",StringType(),True), \
    StructField("PhoneBH",StringType(),True), \
    StructField("PhoneMob",StringType(),True), \
    StructField("Fax",StringType(),True), \
    StructField("Email",StringType(),True), \
    StructField("Internet",StringType(),True), \
    StructField("CompanyId",StringType(),True), \
    StructField("Company",StringType(),True), \
    StructField("Type",StringType(),True), \
    StructField("Active",StringType(),True), \
    StructField("AdrType",StringType(),True), \
    StructField("DeptId",StringType(),True), \
    StructField("Last_Logon",StringType(),True), \
    StructField("CompAdr1",StringType(),True), \
    StructField("CompAdr2",StringType(),True), \
    StructField("Dt_CrtdOn",StringType(),True), \
    ]))

# COMMAND ----------

#Schmea of report 04.001
dfschema04001 = ArrayType(
elementType =StructType([ \
#    StructField("ProjNo",StringType(),True), \
    StructField("Int_Key",StringType(),True), \
    StructField("CheckedOutValue",StringType(),True), \
    StructField("CheckedOut",StringType(),True), \
    StructField("DocumentNo",StringType(),True), \
    StructField("Rev",StringType(),True), \
    StructField("Sts",StringType(),True), \
    StructField("Title",StringType(),True), \
    StructField("Discipline",StringType(),True), \
    StructField("Category",StringType(),True), \
    StructField("Type",StringType(),True), \
    StructField("AvlDwgFmts",StringType(),True), \
    StructField("Int_Apprvd",StringType(),True), \
    StructField("RvwStatus",StringType(),True), \
    StructField("FromId",StringType(),True), \
    StructField("Approved",StringType(),True), \
    StructField("FirstName",StringType(),True), \
    StructField("LastName",StringType(),True), \
    StructField("Company",StringType(),True), \
    StructField("FromUser",StringType(),True), \
    StructField("DocumentSender",StringType(),True), \
    StructField("CheckedOutBy",StringType(),True), \
    StructField("Dt_Checkd",StringType(),True), \
    StructField("Superseded",StringType(),True), \
    StructField("ChkOutAcc",StringType(),True), \
    StructField("DocAccess",StringType(),True), \
    StructField("DwgFormat",StringType(),True), \
    StructField("Format1Access",StringType(),True), \
    StructField("Format2Access",StringType(),True), \
    StructField("Format4Access",StringType(),True), \
    StructField("Format8Access",StringType(),True), \
    StructField("FormatCaption1",StringType(),True), \
    StructField("FormatCaption2",StringType(),True), \
    StructField("FormatCaption3",StringType(),True), \
    StructField("FormatCaption4",StringType(),True), \
    StructField("FormatAccess1",StringType(),True), \
    StructField("FormatAccess2",StringType(),True), \
    StructField("FormatAccess3",StringType(),True), \
    StructField("FormatAccess4",StringType(),True), \
#    StructField("_corrupt_record",StringType(),True), \
    ]))
#Create empty dataframe for report 04.001 to append data from loop call




# COMMAND ----------

#Schmea of report 07.010
dfschema07010 = ArrayType(
elementType =StructType([ \
#     StructField("ProjNo",StringType(),True), \
    StructField("DocumentNo",StringType(),True), \
    StructField("Rev",StringType(),True), \
    StructField("Sts",StringType(),True), \
    StructField("Title",StringType(),True), \
    StructField("Discipline",StringType(),True), \
    StructField("Int_Apprvd",StringType(),True), \
    StructField("CheckedOut",StringType(),True), \
    StructField("Id",StringType(),True), \
    StructField("CompanyId",StringType(),True), \
    StructField("FirstName",StringType(),True), \
    StructField("LastName",StringType(),True), \
    StructField("Int_Adr",StringType(),True), \
    StructField("Company",StringType(),True), \
    StructField("Int_Hst",StringType(),True), \
    StructField("ActComp",StringType(),True), \
    StructField("SchComp",StringType(),True), \
    StructField("Level",StringType(),True), \
    StructField("Sequence",StringType(),True), \
    StructField("TotSchld",StringType(),True), \
    StructField("TotTaken",StringType(),True), \
    StructField("TotLeft",StringType(),True), \
    StructField("RvwType",StringType(),True), \
    StructField("Notified",StringType(),True), \
    StructField("Dt_ReqBy",StringType(),True), \
    StructField("ReviewBy",StringType(),True), \
    StructField("Optional",StringType(),True), \
    StructField("AutoRvwed",StringType(),True), \
    StructField("RvwStsCode",StringType(),True), \
    StructField("RvwTitle",StringType(),True), \
    StructField("AutoRvwSum",StringType(),True), \
    StructField("CancelCmnt",StringType(),True), \
    StructField("CancelBy",StringType(),True), \
    StructField("Int_SndCmp",StringType(),True), \
    StructField("ReviewerName",StringType(),True), \
    StructField("ReviewerCompany",StringType(),True), \
    StructField("RvwCoordinatorName",StringType(),True), \
    StructField("Actual_CompletedBy",StringType(),True), \
    StructField("RoleCompany",StringType(),True), \
    StructField("ReviewRoleName",StringType(),True), \
    StructField("RoleCompanyID",StringType(),True), \
    StructField("RoleUserID",StringType(),True), \
    StructField("ReviewRoleFirstName",StringType(),True), \
    StructField("ReviewRoleLastName",StringType(),True), \
    StructField("ReviewRoleUserIntKey",StringType(),True), \
    StructField("cordinatorRoleName",StringType(),True), \
    StructField("RoleID",StringType(),True), \
    StructField("ReviewCmntCount",StringType(),True), \
    StructField("Notes",StringType(),True), \
    ]))


# COMMAND ----------

#Fuction to get access token to get report data
def udf_GetInEightAPIAccessToken(prmUserID,prmCompanyID,prmPassword,prmApplicationName,prmProjectNo):
    payload=""
    url = "https://au2.doc.ineight.com/TBReportingAPI/session"

    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'  }

    payload=f"UserID={prmUserID}&CompanyID={prmCompanyID}&Password={prmPassword}&ProjectNo={prmProjectNo}&Application={prmApplicationName}"
    
    response = requests.request("POST", url, headers=headers, data=payload)
    token="Basic {}".format(response.text.replace('"',''))
    return token

# COMMAND ----------

#Function to retrieve project and report details
def udf_GetinEightProjectData(prmCompanyID,prmUserID,prmPassword):
    url = f"https://au2.doc.ineight.com/TBReportingAPI/Company/{prmCompanyID}/User/{prmUserID}/Project"
    headers = {
      'Authorization': f"Basic {prmPassword}",
      'Content-Type': 'application/json'  }
    response = requests.request("GET", url, headers=headers)
    if response.status_code==200 and response != None:
        json_response = json.loads(response.text)
        projectdf = spark.createDataFrame(json_response, dfschemaprojectlist)
        # projectdf=spark.read.schema(dfschemaprojectlist).json(sc.parallelize([response.text]))
        projectdf.createOrReplaceTempView("tb_projectlist")
        sqlcmd=f"drop table if exists {get_env()}raw.ineight.tb_projectlist;"
        spark.sql(sqlcmd)      
        sqlcmd=f"create table {get_env()}raw.ineight.tb_projectlist as select * from tb_projectlist;"
        spark.sql(sqlcmd)       
        returndf=projectdf.select("ProjNo") \
                          .withColumnRenamed("ProjNo","ProjectNo")
    else:
        returndf=None
   
    return returndf

# COMMAND ----------

#Function to retrieve report data
def udf_GetInEightReportData(prmAccessToken,prmReportID):

    url=f"https://au2.doc.ineight.com/TBReportingAPI/Report/{prmReportID}"
    header={
        'Authorization':prmAccessToken
    }
    response = requests.request("GET", url, headers=header)
    if response.status_code==200:
        responsedata=response.text
    else:
        responsedata=None
    #reportdf = spark.read.json(sc.parallelize([response.text]))
    
#     reportdf = spark.read.option("mode", "PERMISSIVE") \
#     .option("columnNameOfCorruptRecord", "_corrupt_record") \
#     .json(sc.parallelize([response.text]))
    
#     data=response.text
    
    return responsedata

# COMMAND ----------

#Get all project numbers to be retrieved
dfprojectlist=udf_GetinEightProjectData(varcompanyid,varuserid,varpassword)

# COMMAND ----------

GetInEightAPIAccessToken=udf(udf_GetInEightAPIAccessToken,StringType())
GetInEightReportData=udf(udf_GetInEightReportData,StringType())

for reportno in varreportnos:
    if reportno=="01.030":
        schema=dfschema01030
    elif reportno=="04.001":
        schema=dfschema04001
    else:
        schema=dfschema07010
    
    dfbase=dfprojectlist.withColumn("UserID",lit(varuserid)) \
                        .withColumn("CompanyID",lit(varcompanyid)) \
                        .withColumn("Password",lit(varpassword)) \
                        .withColumn("ApplicationID",lit(varapplication)) \
                        .withColumn("ReportNo",lit(reportno)) \
                        .withColumn("AccessToken", \
                                    GetInEightAPIAccessToken('UserID','CompanyID','Password','ApplicationID','ProjectNo')) \
                        .withColumn("Data",GetInEightReportData('AccessToken','ReportNo')) \
                        .filter(col("Data").isNotNull()) \
                        .withColumn("DF",from_json(col("Data"),schema)) \
                        .withColumn("DF",explode("DF")) \
                        .select("ProjectNo","ReportNo","DF.*")

    objectname=reportno.replace('.','')
    temptablename=f"tb_{objectname}"
    dfbase.createOrReplaceTempView(temptablename)
    sqlcmd=f"drop table if exists {get_env()}raw.ineight.tb_report{objectname};"
    spark.sql(sqlcmd)      
    sqlcmd=f"create table {get_env()}raw.ineight.tb_report{objectname} as select * from {temptablename};"
    spark.sql(sqlcmd)

# COMMAND ----------

spark.sql(f"""drop table if exists {get_env()}raw.ineight.tb_report07010""")


# COMMAND ----------

for table_name in ('tb_projectlist','tb_report01030', 'tb_report04001'):
    spark.sql(f"""CREATE VIEW if not exists {get_env()}cleansed.ineight.{table_name} AS SELECT * FROM {get_env()}raw.ineight.{table_name}""")
    print(f"*******VIEW Created {get_env()}cleansed.ineight.{table_name}")
