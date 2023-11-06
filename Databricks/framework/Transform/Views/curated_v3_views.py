# Databricks notebook source
table_list = [
"dimaccountbusinesspartner"
# ,"dimbusinesspartner"
,"dimbusinesspartneraddress"
,"dimbusinesspartnergroup"
,"dimbusinesspartneridentification"
,"dimbusinesspartnerrelation"
,"dimcontract"
,"dimcontractaccount"
,"dimcontracthistory"
# ,"dimdate"
,"dimdevice"
,"dimdevicecharacteristics"
,"dimdevicehistory"
,"dimdeviceinstallationhistory"
,"dimdisconnectiondocument"
,"diminstallation"
,"diminstallationfacts"
,"diminstallationhistory"
,"dimlocation"
,"dimmeterconsumptionbillingdocument"
,"dimmeterconsumptionbillinglineitem"
,"dimproperty"
,"dimpropertylot"
,"dimpropertyrelation"
,"dimpropertyservice"
,"dimpropertytypehistory"
,"dimregisterhistory"
,"dimregisterinstallationhistory"
,"dimsewernetwork"
,"dimstormwaternetwork"
,"dimwaternetwork"
,"factbilledwaterconsumption"
,"factmonthlyapportionedconsumption"
,"viewbusinesspartner"
,"viewbusinesspartnergroup"
,"viewcontract"
,"viewcontractaccount"
,"viewdevice"
,"viewdevicecharacteristics"
,"viewinstallation"
,"viewproperty"
,"viewpropertyrelation"
,"viewpropertyservice"
]

# COMMAND ----------

base_schema = "curated_v2"
new_schema = "curated_v3"

# COMMAND ----------

for table in table_list:
    try:
        spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {new_schema}""")
        spark.sql(f"""CREATE OR REPLACE VIEW {new_schema}.{table} AS SELECT * FROM {base_schema}.{table}""")
        print(f"*******VIEW Created {new_schema}.{table}")
    except Exception as e:
        print(f"*******VIEW Creation FAIL {base_schema}.{table} Error: {e}")
        pass
  

# COMMAND ----------


