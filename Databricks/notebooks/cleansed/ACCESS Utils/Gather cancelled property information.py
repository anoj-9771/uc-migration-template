# Databricks notebook source
# MAGIC %md
# MAGIC Fields to acquire for cancelled ratings:<br >
# MAGIC <ul>
# MAGIC   <li>dimLocation - LGA</li>
# MAGIC   <li>dimLocation - latitude</li>
# MAGIC   <li>dimLocation - longitude</li>
# MAGIC   <li>dimProperty - waterNetworkSK_drinkingWater</li>
# MAGIC   <li>dimProperty - waterNetworkSK_recycledWater</li>
# MAGIC   <li>dimProperty - sewerNetworkSK</li>
# MAGIC   <li>dimProperty - stormWaterNetworkSK</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cancelledProps as
# MAGIC select propertyNumber, null as activeProperty
# MAGIC from   curated.dimProperty
# MAGIC where  propertyTypeCode = '998'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view proplist as
# MAGIC with t1 as (select pp.previousPropertyNumber as oldProp, pr1.propertyTypeCode as oldPropType, 
# MAGIC                    pp.propertyNumber as newProp, pr2.propertyTypeCode as newPropType,
# MAGIC                    row_number() over (partition by previousPropertyNumber order by pr2.propertyTypeCode) as rn
# MAGIC             from   cleansed.access_z309_tprevproperty pp, 
# MAGIC                    cleansed.access_z309_tproperty pr1, 
# MAGIC                    cleansed.access_z309_tproperty pr2
# MAGIC             where  pp.previousPropertyNumber = pr1.propertyNumber
# MAGIC             and    pp.propertyNumber = pr2.propertyNumber
# MAGIC             and    pp.propertyNumber != pp.previousPropertyNumber
# MAGIC             and    pr1.propertyTypeCode = '998'
# MAGIC             )
# MAGIC select oldProp, oldPropType, newProp, newPropType 
# MAGIC from t1 
# MAGIC where rn = 1
# MAGIC ORDER BY 1,3,4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct previousPropertyNumber) from  cleansed.access_z309_tprevproperty--propList

# COMMAND ----------

propList = spark.sql('select * from propList').rdd.map(lambda row: row.asDict()).collect()
dfAddress = spark.sql('select distinct pa.* \
                        from cleansed.access_z309_tpropertyaddress pa, \
                             cleansed.access_z309_tprevproperty pp \
                        where pa.propertyNumber = pp.previousPropertyNumber \
                        or    pa.propertyNumber = pp.propertyNumber')


# COMMAND ----------

propDict = {}
for row in propList:
    propDict[row['oldProp']] = [row['newProp'], row['newPropType']] 
# propDict

# COMMAND ----------

def getActiveProp(cancelledProperty):
    if propDict.get(cancelledProperty,[0,'x'])[1] == '998':
        newProp = getActiveProp(propDict[cancelledProperty][0])
    else:
        newProp = propDict.get(cancelledProperty,[0,'x'])[0]

    return newProp
            


# COMMAND ----------

referenceList = []
for row in propList:
#     print(propDict.get(row['oldProp'],[0,'x'])[1])
    referenceList.append([row['oldProp'],getActiveProp(row['oldProp'])])
#     break
cols = ['propertyNumber', 'activeProperty']
dfProps = spark.createDataFrame(referenceList, cols)
dfProps.createOrReplaceTempView('fromPrevProps')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fromprevprops

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cancelledProps2 as
# MAGIC select propertyNumber, null as activeProperty
# MAGIC from   cancelledProps cp
# MAGIC where propertyNumber not in (select propertyNumber from fromPrevProps pp where pp.activeProperty > 0)
# MAGIC union all
# MAGIC select cp.propertyNumber, pp.activeProperty
# MAGIC from   cancelledProps cp, fromPrevProps pp 
# MAGIC where pp.propertyNumber = cp.propertyNumber 
# MAGIC and   pp.activeProperty > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cancelledProps2
# MAGIC where activeproperty is not null

# COMMAND ----------


