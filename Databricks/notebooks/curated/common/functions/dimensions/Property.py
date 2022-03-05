# Databricks notebook source
###########################################################################################################################
# Function: getProperty
#  GETS Property DIMENSION 
# Returns:
#  Dataframe of transformed Property
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getProperty():

#     spark.udf.register("TidyCase", GeneralToTidyCase)  

    #dimProperty
    #2.Load Cleansed layer table data into dataframe
    accessZ309TpropertyDf = spark.sql(f"select cast(propertyNumber as string), \
                                            'ACCESS' as sourceSystemCode, \
                                            propertyTypeCode, \
                                            propertyType, \
                                            superiorPropertyTypeCode, \
                                            superiorPropertyType, \
                                            CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
                                                                                  ELSE propertyArea END AS areaSize, \
                                            '0' as parentPropertyNumber, \
                                            null as parentPropertyTypeCode, \
                                            null as parentPropertyType, \
                                            null as parentSuperiorPropertyTypeCode, \
                                            null as parentSuperiorPropertyType, \
                                            null as planTypeCode, \
                                            null as planType, \
                                            null as lotTypeCode, \
                                            null as lotType, \
                                            null as planNumber, \
                                            null as lotNumber, \
                                            null as sectionNumber, \
                                            null as architecturalTypeCode, \
                                            null as architecturalType \
                                     from {ADS_DATABASE_CLEANSED}.access_z309_tproperty \
                                     ")

    sapisuDf = spark.sql(f"select co.propertyNumber, \
                                'ISU' as sourceSystemCode, \
                                null as propertyTypeCode, \
                                null as propertyType, \
                                null as superiorPropertyTypeCode, \
                                null as superiorPropertyType, \
                                CASE WHEN vd.hydraAreaUnit == 'HAR' THEN cast(vd.hydraCalculatedArea * 10000 as dec(18,6)) \
                                     WHEN vd.hydraAreaUnit == 'M2'  THEN cast(vd.hydraCalculatedArea as dec(18,6)) \
                                                                    ELSE null END AS areaSize, \
                                vn.parentArchitecturalObjectNumber as parentPropertyNumber, \
                                pa.inferiorPropertyTypeCode as parentPropertyTypeCode, \
                                pa.inferiorPropertyType as parentPropertyType, \
                                pa.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                pa.superiorPropertyType as parentSuperiorPropertyType, \
                                co.planTypeCode, \
                                co.planType, \
                                co.lotTypeCode, \
                                dt.domainValueText as lotType, \
                                co.planNumber, \
                                co.lotNumber, \
                                co.sectionNumber, \
                                co.architecturalObjectTypeCode as architecturalTypeCode, \
                                co.architecturalObjectType as architecturalType \
                        from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 co left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_vibdao vd on co.architecturalObjectInternalId = vd.architecturalObjectInternalId and vd._RecordDeleted = 0 \
                              and vd._RecordCurrent = 1 left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_vibdnode vn on co.architecturalObjectInternalId = vn.architecturalObjectInternalId and vn._RecordDeleted = 0 \
                              and   vn._RecordCurrent = 1 left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 pa on vn.parentArchitecturalObjectInternalId = pa.architecturalObjectInternalId  \
                              and   pa._RecordCurrent = 1 and pa._RecordDeleted = 0 left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_dd07t dt on co.lotTypeCode = dt.domainValueSingleUpperLimit and domainName = 'ZCD_DO_ADDR_LOT_TYPE' \
                              and dt._RecordDeleted = 0 and   dt._RecordCurrent = 1 \
                         where co._RecordDeleted = 0 \
                         and   co._RecordCurrent = 1 \
                        ")

    #Dummy Record to be added to Property Dimension
    dummyDimRecDf = spark.createDataFrame([("ISU","-1"),("ACCESS","-2"),("ISU","-3"),("ACCESS","-4")], ["sourceSystemCode", "propertyNumber"])

    #3.JOIN TABLES  
    #4.UNION TABLES
    df = accessZ309TpropertyDf.union(sapisuDf)
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    print(f'{df.count():,} rows after Union 2')

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
     "propertyNumber" \
    ,"sourceSystemCode" \
    ,"propertyTypeCode" \
    ,"propertyType" \
    ,"superiorPropertyTypeCode" \
    ,"superiorPropertyType" \
    ,"areaSize" \
    ,'parentPropertyNumber' \
    ,'parentPropertyTypeCode' \
    ,'parentPropertyType' \
    ,'parentSuperiorPropertyTypeCode' \
    ,'parentSuperiorPropertyType' \
    ,'planTypeCode' \
    ,'planType' \
    ,'lotTypeCode' \
    ,'lotType' \
    ,'planNumber' \
    ,'lotNumber' \
    ,'sectionNumber' \
    ,'architecturalTypeCode' \
    ,'architecturalType' \
    )
                                            
    df.createOrReplaceTempView('allproperties')
    #6.Apply schema definition
    newSchema = StructType([
                            StructField("propertyNumber", StringType(), False),
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("propertyTypeCode", StringType(), True),
                            StructField("propertyType", StringType(), True),
                            StructField("superiorPropertyTypeCode", StringType(), True),
                            StructField("superiorPropertyType", StringType(), True),
                            StructField("areaSize", DecimalType(18,6), True),
                            StructField('parentPropertyNumber', StringType(), True),
                            StructField('parentPropertyTypeCode', StringType(), True),
                            StructField('parentPropertyType', StringType(), True),
                            StructField('parentSuperiorPropertyTypeCode', StringType(), True),
                            StructField('parentSuperiorPropertyType', StringType(), True),
                            StructField('planTypeCode', StringType(), True),
                            StructField('planType', StringType(), True),
                            StructField('lotTypeCode', StringType(), True),
                            StructField('lotType', StringType(), True),
                            StructField('planNumber', StringType(), True),
                            StructField('lotNumber', StringType(), True),
                            StructField('sectionNumber', StringType(), True),
                            StructField('architecturalTypeCode', StringType(), True),
                            StructField('architecturalType', StringType(), True),
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df


# COMMAND ----------

# ISUDummy = tuple(['-1','ISU','1900-01-01','2099-12-31'] + ['Unknown'] * 4 + [0] + ['Unknown'] * (len(sapisuDf.columns) - 9)) #this only works as long as all output columns are string
#     ACCESSDummy = tuple(['-2','ACCESS','1900-01-01','2099-12-31'] + ['Unknown'] * 4 + [0] + ['Unknown'] * (len(sapisuDf.columns) - 9)) #this only works as long as all output columns are string
#     dummyDimRecDf = spark.createDataFrame([ISUDummy, ACCESSDummy], sapisuDf.columns)
#     dummyDimRecDf = dummyDimRecDf.withColumn("propertyStartDate",dummyDimRecDf['propertyStartDate'].cast(DateType())).withColumn("propertyEndDate",dummyDimRecDf['propertyEndDate'].cast(DateType()))
