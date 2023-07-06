# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def create_temp_table(dataframe):
    spark.sql("create schema if not exists temp")
    spark.sql("drop table if exists temp.dimAsset_temp")
    temp_df = dataframe
    
    asset_location_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimAssetLocation')}").filter("_RecordDeleted == 0").select(col("assetLocationName").alias("location"),"assetLocationSK","assetLocationTypeCode","assetLocationFacilityShortCode")
    class_structure_df = GetTable(get_table_name(f"{SOURCE}","maximo","classStructure")).filter("_RecordDeleted == 0").select("classStructure","classification",col("description").alias("classificationPath"))
    classification_df = GetTable(get_table_name(f"{SOURCE}","maximo","classification")).filter("_RecordDeleted == 0").select("classification",col("description").alias("classificationDescription"))
    astmeter_df = GetTable(get_table_name(f"{SOURCE}","maximo","assetMeter")).filter("_RecordDeleted == 0").filter("meter in ('CAG_ROMP','EXPTOTALLIFE')").select("asset","meter", "lastReading")

    pivot_meter_df = astmeter_df.groupBy("asset").pivot("meter").agg(min(col("lastReading")))
    
    locspc_df = spark.sql(f"""select lsp.location, lsp.numericValue as loc_numericValue from {get_table_name(f"{SOURCE}","maximo","locationspec")} lsp where lsp.attribute = 'COF_SCORE' and _RecordDeleted = 0""").select("location","loc_numericValue")
    
    asset_spec_df =spark.sql(f"""select * except(rownumb) from ((select asset, attribute, alphanumericValue as val, row_number() over(partition by asset,attribute order by changedDate desc) as rownumb from {get_table_namespace('cleansed', 'maximo_assetspec')} where attribute in ("MAIN_TYPE","SEWER_FUNCTION","PURPOSE","PIPE_SIZE","VALVE_SIZE","HORIZONTAL_LENGTH","SEWER_MATERIAL","WATER_TYPE","LATESTREHABTYPE","CROSS_SECTION","MAINTENANCE_STRATEGY","WATERMAIN_PIPETYPE") and _RecordDeleted = 0)
    UNION
    (select asset, attribute, numericValue as val, row_number() over(partition by asset,attribute order by changedDate desc) as rownumb from {get_table_namespace('cleansed', 'maximo_assetspec')} where attribute = "COF_SCORE" and _RecordDeleted = 0)
    )dt where rownumb = 1""".format(get_table_name(f"{SOURCE}","maximo","assetspec")))
    pivot_df = asset_spec_df.groupBy("asset").pivot("attribute").agg(min(col("val")))
    
    # ------------- JOINS ------------------ #    
    temp_df = temp_df.join(asset_location_df,"location","left") \
    .join(locspc_df,"location","left") \
    .join(class_structure_df,"classStructure","left") \
    .join(classification_df,"classification","left") \
    .join(pivot_meter_df,"asset","left") \
    .join(pivot_df,"asset","left") \
    .withColumn("assetLinearMaintenanceFlag",when(temp_df.masteredInGis == 'Y','Y').when(asset_location_df.assetLocationTypeCode == "SYSAREA",'Y').otherwise('N'))
    
    temp_df = temp_df.withColumn("assetSpecMainTypeName",temp_df.MAIN_TYPE)\
    .withColumn("assetSewerFunctionName",temp_df.SEWER_FUNCTION)\
    .withColumn("assetSewerPurposeValue",temp_df.PURPOSE)\
    .withColumn("assetPipeSizeValue",temp_df.PIPE_SIZE)\
    .withColumn("assetValveSizeValue",temp_df.VALVE_SIZE)\
    .withColumn("assetHorizontalLengthValue",temp_df.HORIZONTAL_LENGTH)\
    .withColumn("assetSewerPipeIdentifier",temp_df.SEWER_MATERIAL)\
    .withColumn("assetWaterTypeIdentifier",temp_df.WATER_TYPE)\
    .withColumn("assetRehabTypeIdentifier",temp_df.LATESTREHABTYPE)\
    .withColumn("assetCrossSectionDescription",temp_df.CROSS_SECTION)\
    .withColumn("assetMaintenanceStrategyDescription",temp_df.MAINTENANCE_STRATEGY)\
    .withColumn("assetWaterMainPipeTypeCode",temp_df.WATERMAIN_PIPETYPE)\
    .withColumn("asp_numericValue",temp_df.COF_SCORE).alias("asset") 

    temp_df = temp_df \
    .withColumn("assetConsequenceOfFailureScoreCode",expr("case when asset.assetLocationTypeCode in ('FACILITY', 'PROCESS','FUNCLOC') then nvl(asset.loc_numericValue,0) \
    when asset.assetLocationTypeCode = 'SYSAREA' \
    then nvl(asset.asp_numericValue,0) end").cast('decimal(38,18)')) \
    .withColumn("assetNetworkLengthPerKilometerValue",expr("case when asset.assetHorizontalLengthValue is null then 0 else asset.assetHorizontalLengthValue/1000 end")) \
    .withColumn("assetConditionalGradeAssessmentValue",expr("NVL(asset.CAG_ROMP, 0)")) \
    .withColumn("assetExpectedServiceLifeCode",temp_df.EXPTOTALLIFE)
    
    temp_df = temp_df.withColumn("ref_join", expr("COALESCE(asset.assetLocationFacilityShortCode,'null')||COALESCE(asset.assetWaterMainPipeTypeCode,'null')||COALESCE(asset.assetSewerFunctionName,'null')||COALESCE(asset.assetSewerPurposeValue,'null')"))
    temp_df.write.saveAsTable("temp.dimAsset_temp")
    print("temp.dimAsset_temp created")

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetChangedTimestamp"
    windowSpec  = Window.partitionBy("asset")
    create_temp_table(get_recent_records(f"{SOURCE}","maximo_asset",business_date,target_date).withColumn("rank",rank().over(windowSpec.orderBy(col(business_date).desc()))).filter("rank == 1").drop("rank"))
    df = spark.sql(f"""select
        da.*, COALESCE(rrc.return1Code,rrc2.return1Code,"Other") as assetTypeGroupDescription,
        case
        when atc.assetTypeClass is NULL and da.assetLinearMaintenanceFlag = 'Y' then 'Linear Asset'
        when da.assetLinearMaintenanceFlag <> 'Y' and da.assetLocationTypeCode = 'SYSAREA' then 'Linear Asset'
        when atc.assetTypeClass is not NULL then atc.assetTypeClass
        end assetTypeClass

        from temp.dimAsset_temp da
        
        left join {get_table_namespace(f'{DEFAULT_TARGET}', 'refreportconfiguration')} rrc
        on rrc.mapTypeCode = 'Asset Type'
        and trim((coalesce(da.assetLocationFacilityShortCode,''))||trim(coalesce(da.assetSpecMainTypeName,''))
        ||trim(coalesce(da.assetSewerFunctionName,''))||trim(coalesce(da.assetSewerPurposeValue,'')))=
        trim(COALESCE(rrc.lookup1Code, da.assetLocationFacilityShortCode,''))||trim(COALESCE(rrc.lookup2Code, da.assetSpecMainTypeName,''))
        ||trim(COALESCE(rrc.lookup3Code, da.assetSewerFunctionName,''))||trim(COALESCE(rrc.lookup4Code, da.assetSewerPurposeValue,''))
        
        left join {get_table_namespace(f'{DEFAULT_TARGET}', 'refreportconfiguration')} rrc2
        on rrc2.mapTypeCode = 'Asset Type 2'
        and rrc2.lookup4Code = da.assetSewerPurposeValue
        and rrc.return1Code is NULL
        
        left join (select 
        lookup1Code as assetFacilityTypeCode, 
        return1Code as assetTypeClass
        from {get_table_namespace(f'{DEFAULT_TARGET}', 'refreportconfiguration')}
        where mapTypeCode = 'Facility Type') atc
        on da.assetLocationFacilityShortCode = assetFacilityTypeCode""")                                                                  
    df = df \
    .withColumn("sourceBusinessKey",df.asset) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df,business_date)
    
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"assetLocationSK assetLocationFK"
        ,"asset assetNumber"
        ,"description assetDescription"
        ,"status assetStatusDescription"
        ,"masteredInGis assetMasteredInGISIndicator"
        ,"cmelCode assetCivilMechanicalElectricIdentificationCode"
        ,"installationDate assetInstallationDate"
        ,"mainAsset assetMasterIndicator"
        ,"maintenanceStrategy assetMaintenanceCode"
        ,"serviceContract assetServiceContractText"
        ,"serviceType assetServiceTypeCode"
        ,"classification assetClassificationCode"
        ,"classificationPath assetClassificationPathDescription"
        ,"classificationDescription assetClassificationDescription"
        ,"assetLinearMaintenanceFlag"
        ,"assetSpecMainTypeName"
        ,"assetSewerFunctionName"
        ,"assetSewerPurposeValue"
        ,"assetPipeSizeValue"
        ,"assetValveSizeValue"
        ,"assetHorizontalLengthValue"
        ,"assetConditionalGradeAssessmentValue"
        ,"assetSewerPipeIdentifier"
        ,"assetWaterTypeIdentifier"
        ,"assetRehabTypeIdentifier"
        ,"assetCrossSectionDescription"
        ,"assetMaintenanceStrategyDescription"
        ,"assetExpectedServiceLifeCode"
        ,"assetWaterMainPipeTypeCode"
        ,"assetConsequenceOfFailureScoreCode"
        ,"assetNetworkLengthPerKilometerValue"
        ,"assetTypeGroupDescription"
        ,"assetTypeClass"
        ,"changedDate assetChangedTimestamp"
        ,"sourceValidFromTimestamp"
        ,"sourceValidToTimestamp"
        ,"sourceRecordCurrent"
        ,"sourceBusinessKey"    
        
    ]
    df = df.selectExpr(
        _.Transforms
    ).drop_duplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("assetNumber",col("sourceValidFromTimestamp").alias("new_changed_date")),"assetNumber","inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        matched_df =matched_df.withColumn("sourceValidToTimestamp",expr("new_changed_date - INTERVAL 1 SECOND")) \
        .withColumn("sourceRecordCurrent",expr("CAST(0 AS INT)"))

        df = df.unionByName(matched_df.selectExpr(df.columns))
    except Exception as exp:
        print(exp)

#     display(df)
    Save(df)
    #DisplaySelf()
    
pass
Transform()


# COMMAND ----------


