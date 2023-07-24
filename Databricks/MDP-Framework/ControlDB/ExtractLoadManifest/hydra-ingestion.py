# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "hydra"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark.sql.functions import col, expr, when, desc

fsSchema = StructType([
    StructField('path', StringType()),
    StructField('name', StringType()),
    StructField('size', LongType()),
    StructField('modificationTime', LongType())
])

def DataFrameFromFilePath(path):
    list = dbutils.fs.ls(path)
    df = spark.createDataFrame(list, fsSchema).withColumn("modificationTime", expr("from_unixtime(modificationTime / 1000)"))
    return df

df = DataFrameFromFilePath("/mnt/datalake-landing/hydra")
df.display()

# COMMAND ----------

sqlBase = """
    with _Base as (SELECT 'hydra' SourceSchema, '' SourceKeyVaultSecret, 'skip-load' SourceHandler, 'json' RawFileExtension, 'raw-load-autoloader' RawHandler, '' ExtendedProperties, 'cleansed-load-explode-json' CleansedHandler, '' WatermarkColumn, 1 Enabled) 
    select '' SourceQuery, '' SourceTableName, * from _base where 1 = 0
    """
sqlLines = ""

for i in df.collect():
    fileName = i.name.replace("/","")
    folderPath = i.path.split("dbfs:")[1]
    sqlLines += f"UNION ALL select '{folderPath}' SourceQuery, '{fileName}' SourceTableName, * from _Base "

print(sqlBase + sqlLines)

df = spark.sql(sqlBase + sqlLines)
df.display()

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

ConfigureManifest(df)   

# COMMAND ----------

ExecuteStatement("""
update dbo.extractLoadManifest set
rawPath = replace(rawPath,'json','csv')
,cleansedHandler = 'cleansed-load-delta'
where systemCode = 'hydra' and sourceTableName in ('tlotparcel','tsystemarea')
""")

ExecuteStatement("""
update dbo.extractLoadManifest set
extendedProperties = '{\"separator\":\"|\"}'
where systemCode = 'hydra' and sourceTableName = 'tlotparcel'
""")

ExecuteStatement("""
update dbo.extractLoadManifest set
extendedProperties = '{\"charset\":\"US-ASCII\"}'
where systemCode = 'hydra' and sourceTableName in ('fm_ground','sampling_point')
""")

#Railway name annotation is completely empty. This breaks the cleansing. Exclude from ingestion until there is data
ExecuteStatement("""
update dbo.extractLoadManifest set
enabled = 0
where systemCode = 'hydra' and sourceTableName = 'railway_name_annotation'
""")

# COMMAND ----------

ExecuteStatement(f"""
update dbo.extractLoadManifest set businessKeyColumn = case sourceTableName
when 'project_construction_coverage' then 'rwoId'
when 'road_qualifier_catalogue' then 'qualifier1'
when 'stormwater_comment' then 'systemKey'
when 'watermain_envirnmnt_annotation' then 'rwoId'
when 'lic_poly' then 'systemKey'
when 'asset_number_annotation' then 'systemKey'
when 'parks_reserves_coverage' then 'rwoId'
when 'sewer_limit_boundary' then 'rwoId'
when 'lic_edge' then 'systemKey'
when 'asset_number' then 'systemKey'
when 'isolatable_section' then 'shutdownNumber'
when 'sewer_lp_pump_unit' then 'systemKey'
when 'water_valve_unit' then 'systemKey'
when 'shutdown' then 'systemKey'
when 'lot_house_number_annotation' then 'rwoId'
when 'stored_area' then 'systemKey'
when 'railway_segment' then 'systemKey'
when 'stormwater_size_type_annotation' then 'rwoId'
when 'brunnel' then 'systemKey'
when 'aquatic_code_annotation' then 'rwoId'
when 'road_large_qualifier2_in_lot' then 'rwoId'
when 'road_name_in_lot' then 'rwoId'
when 'sewer_system_catchment' then 'systemKey'
when 'stormwater_pollution_trap' then 'systemKey'
when 'major_works_area_coverage' then 'rwoId'
when 'iworcs_contact_catalogue' then 'id'
when 'parks_and_reserves' then 'name'
when 'sewer_system_coverage' then 'rwoId'
when 'legend_entry' then 'systemKey'
when 'waterway_route' then 'rwoId'
when 'alt_road_qualifier_catalogue' then 'altQualifier1'
when 'postcode_annotation' then 'rwoId'
when 'drawing_template' then 'name'
when 'historical_dsp_coverage' then 'rwoId'
when 'system_area_hier_catalogue' then 'level,product'
when 'ext_stormwater_channel' then 'systemKey'
when 'landscape_soil_codes_catalogue' then 'systemKey'
when 'sewer_size_type_catalogue' then 'pipeSize,pipeJointingMethod,pipeClass,function,pipeType'
when 'fm_ground_nature_strip' then 'systemKey'
when 'water_canal_annotation' then 'rwoId'
when 'place_of_interest_type_catalogue' then 'qualifier'
when 'system_area_catalogue' then 'system,number'
when 'special_use_area_coverage' then 'rwoId'
when 'stormwater_enquiry_number' then 'systemKey'
when 'water_structure' then 'systemKey'
when 'dsp' then 'systemKey'
when 'dp_number' then 'systemKey'
when 'pseudo_lot' then 'propertyNumber'
when 'trunk_main_details' then 'systemKey'
when 'waterway_name_annotation' then 'rwoId'
when 'fme_translation_complex' then 'id'
when 'intersection' then 'systemKey'
when 'water_meter' then 'systemKey'
when 'sewer_lp_alarm_location' then 'rwoId'
when 'stormwater_name_annotation' then 'rwoId'
when 'pseudo_sewer_section' then 'systemKey'
when 'railway_annotation' then 'rwoId'
when 'contact' then 'systemKey'
when 'local_government_area' then 'systemKey'
when 'sewer_junction_detail' then 'systemKey'
when 'sewer_purpose_annotation' then 'rwoId'
when 'sewer_structure' then 'systemKey'
when 'land_development_area_coverage' then 'rwoId'
when 'state_survey_mark' then 'systemKey'
when 'stormwater_inspection_details' then 'systemKey'
when 'suburb_name' then 'systemKey'
when 'postcode_coverage' then 'rwoId'
when 'govt_land_housing_coverage' then 'rwoId'
when 'city_packet_number' then 'systemKey'
when 'fm_ground_fire_trail' then 'systemKey'
when 'sewer_chemical_dosing_unit' then 'systemKey'
when 'iff_facility_field' then 'fieldSequence,swRwoName,mapping'
when 'major_works_area_annotation' then 'rwoId'
when 'billable_location' then 'systemKey'
when 'cadastral_control_point' then 'lotNumber,pointNumber,planNumber'
when 'special_use_area' then 'systemKey'
when 'stratum_details' then 'systemKey'
when 'sewer_owner_annotation' then 'rwoId'
when 'traverse' then 'name'
when 'waterway_qualifier_annotation' then 'rwoId'
when 'cma_sheets' then 'sheetNumber'
when 'fm_ground_coverage' then 'rwoId'
when 'special_use_area_annotation' then 'rwoId'
when 'easement_name2_annotation' then 'rwoId'
when 'parks_and_reserves_catalogue' then 'systemKey'
when 'sw_internal_reference' then 'systemKey'
when 'easement_owner_catalogue' then 'owner'
when 'zoning' then 'id'
when 'water_system_area_coverage' then 'rwoId'
when 'iff_facility_defn' then 'mapping,swRwoName'
when 'suburb_annotation' then 'rwoId'
when 'road_segment' then 'systemKey'
when 'fm_building' then 'systemKey'
when 'stormwater_owner_catalogue' then 'owner'
when 'stormwater_valve' then 'systemKey'
when 'lot_number_annotation' then 'rwoId'
when 'change_metadata' then 'id'
when 'land_development_area_annotation' then 'rwoId'
when 'plot_request' then 'createdBy,requestName'
when 'critical_information_annotation' then 'rwoId'
when 'stormwater_system_catchment' then 'systemKey'
when 'road_qualifier1_in_lot' then 'rwoId'
when 'suburb_coverage' then 'rwoId'
when 'urban_development_route' then 'rwoId'
when 'stormwater_planno_annotation' then 'rwoId'
when 'service_provider_polygon' then 'systemKey'
when 'billable_loc_plan_type_dyn_enum' then 'billableLocationPlanType'
when 'waterway' then 'systemKey'
when 'railway' then 'systemKey'
when 'construction' then 'rwoId'
when 'sewer_limit' then 'systemKey'
when 'audit_trail' then 'id'
when 'easement_dp_annotation' then 'rwoId'
when 'original_asset_transaction' then 'id'
when 'easement' then 'systemKey'
when 'stormwater_node' then 'systemKey'
when 'urban_development_area' then 'systemKey'
when 'landscape_and_soil_types' then 'systemKey'
when 'intersecting_road' then 'systemKey'
when 'stormwater_size_type_catalogue' then 'pipeType,pipeSize'
when 'audit_field_value' then 'id'
when 'railway_qualifier_annotation' then 'rwoId'
when 'water_canal' then 'systemKey'
when 'watermain_extent' then 'rwoId'
when 'fm_ground_grass_area' then 'systemKey'
when 'water_stopvalve' then 'systemKey'
when 'world_isect' then 'worldId,universeId'
when 'email_alias' then 'alias,environment'
when 'hazard_matrix' then 'product,object,codeKey,code,reactiveYn'
when 'iff_facility_index' then 'key,id'
when 'road_qualifier2_in_lot' then 'rwoId'
when 'sewer_rehabilitation_detail' then 'systemKey'
when 'service_provider_catalogue' then 'serviceProvider'
when 'sewer_size_type_annotation' then 'rwoId'
when 'sewer_electrical_cable_route' then 'rwoId'
when 'census_area_annotation' then 'rwoId'
when 'error_log' then 'errorRecordNo'
when 'ext_stormwater_valve' then 'systemKey'
when 'heritage_code_annotation' then 'rwoId'
when 'sewer_structure_rehab_detail' then 'systemKey'
when 'watermain_size_type_annotation' then 'rwoId'
when 'service_provider' then 'systemKey'
when 'road_name_on_centreline' then 'rwoId'
when 'urban_development_annotation' then 'rwoId'
when 'aquatic_catalogue' then 'systemKey'
when 'load_file_log' then 'id'
when 'stormwater_system_annotation' then 'rwoId'
when 'raster_map' then 'systemKey,sheetRef'
when 'plot_theme' then 'name,createdBy,themeType'
when 'water_chemical_dosing_unit' then 'systemKey'
when 'ext_stormwater_area' then 'systemKey'
when 'fm_ground_bushland_area' then 'systemKey'
when 'shutdown_instruction' then 'systemKey'
when 'drafting_lines' then 'rwoId'
when 'colour_raster' then 'systemKey'
when 'census_area_data' then 'systemKey'
when 'water_canal_extent' then 'rwoId'
when 'asset_cof_score' then 'id'
when 'sewer_valve_size_catalogue' then 'valveSize'
when 'fm_ground' then 'systemKey'
when 'sewer_system_annotation' then 'rwoId'
when 'water_conn_size_type_catalogue' then 'pipeType,pipeSize'
when 'road' then 'systemKey'
when 'low_water_pressure_rebate_area' then 'systemKey'
when 'watermain_special_annotation' then 'systemKey'
when 'fm_ground_boundary' then 'systemKey'
when 'lga_coverage' then 'rwoId'
when 'special_tariff_area' then 'systemKey'
when 'water_connection_point' then 'systemKey'
when 'postcode' then 'systemKey'
when 'stormwater_number_annotation' then 'rwoId'
when 'manual_assigned_assets' then 'assetNumber'
when 'future_dsp_coverage' then 'rwoId'
when 'system_configuration_parameters' then 'value,parameter,applicationCode'
when 'dsp_annotation' then 'rwoId'
when 'drafting_areas' then 'rwoId'
when 'water_system_area_annotation' then 'rwoId'
when 'water_joint' then 'systemKey'
when 'projection' then 'id'
when 'heritage' then 'heritageItemNumber'
when 'ext_stormwater_structure' then 'systemKey'
when 'critical_customer' then 'systemKey'
when 'water_service_connection' then 'systemKey'
when 'water_system_area' then 'systemKey'
when 'sewer_main_hazard' then 'systemKey'
when 'theme_predicate' then 'systemKey'
when 'water_canal_name_detail' then 'systemKey'
when 'sewer_monitoring_point' then 'systemKey'
when 'dsp_coverage' then 'rwoId'
when 'hazard_catalogue' then 'code,object'
when 'water_fitting' then 'systemKey'
when 'fm_ground_surface_drain' then 'systemKey'
when 'ubd_map' then 'systemKey'
when 'sewer_status_annotation' then 'rwoId'
when 'stormwater_extra_enquiry_number' then 'systemKey'
when 'sewer_flow_arrow_location' then 'rwoId'
when 'theme_visibility' then 'systemKey'
when 'linked_document_object' then 'id'
when 'stormwater_other_linework' then 'rwoId'
when 'water_service_point' then 'systemKey'
when 'stormwater_pumping_station' then 'systemKey'
when 'invalid_property_number' then 'systemKey'
when 'water_valve' then 'systemKey'
when 'sewer_main' then 'systemKey'
when 'stormwater_channel_flow_arrow' then 'rwoId'
when 'facility_water_gauge' then 'systemKey'
when 'sewer_overflow_storage_unit' then 'systemKey'
when 'urban_development_coverage' then 'rwoId'
when 'fm_ground_annotation' then 'rwoId'
when 'workorder_geometry' then 'rwoId'
when 'theme_all_test' then 'systemKey'
when 'sewer_rain_gauge' then 'systemKey'
when 'sewer_extra_extent_of_wo' then 'rwoId'
when 'historical_dsp' then 'systemKey'
when 'traverse_segment' then 'systemKey'
when 'sewer_name_annotation' then 'rwoId'
when 'backflow_prevention' then 'systemKey'
when 'pseudo_asset' then 'systemKey'
when 'contact_details' then 'systemKey'
when 'unsewered_area_coverage' then 'rwoId'
when 'backflow_device' then 'systemKey'
when 'sewer_gauge' then 'systemKey'
when 'system_area_coverage' then 'rwoId'
when 'fm_ground_stormwater_pit' then 'systemKey'
when 'stormwater_system_coverage' then 'rwoId'
when 'water_reservoir' then 'systemKey'
when 'change_in_stewardship' then 'systemKey'
when 'road_large_name_in_lot' then 'rwoId'
when 'general_interface' then 'propertyNumberSequence,propertyNumber'
when 'plot_area' then 'systemKey'
when 'water_pump_unit' then 'systemKey'
when 'project_construction_area' then 'name'
when 'sewer_odour_control_unit' then 'systemKey'
when 'railway_name_annotation' then 'rwoId'
when 'sewer_owner_catalogue' then 'owner'
when 'sewer_conditions_for_lot' then 'systemKey'
when 'mp_use' then 'useType,dataSource,id'
when 'water_treatment_works' then 'systemKey'
when 'rural_water_area_coverage' then 'rwoId'
when 'waterway_coverage' then 'rwoId'
when 'lga_annotation' then 'rwoId'
when 'dbyd_object' then 'systemKey'
when 'heritage_catalogue' then 'systemKey'
when 'pipeline_risk' then 'id'
when 'land_use_catalogue' then 'systemKey'
when 'heritage_coverage' then 'rwoId'
when 'affected_lots' then 'systemKey'
when 'special_tariff_area_annotation' then 'rwoId'
when 'drafting_texts' then 'rwoId'
when 'place_of_interest' then 'systemKey'
when 'sewer_flowmeter' then 'systemKey'
when 'proposed_cadastral_annotation' then 'systemKey'
when 'incident' then 'systemKey'
when 'critical_information_coverage' then 'rwoId'
when 'road_large_qualifier1_in_lot' then 'rwoId'
when 'watermain_size_type_catalogue' then 'pipeSize,pipeJointingMethod,pipeClass,pipeType'
when 'rural_water_area_annotation' then 'rwoId'
when 'fm_ground_fencing' then 'systemKey'
when 'linked_document' then 'id'
when 'sewer_name_detail' then 'systemKey'
when 'drafting_points' then 'rwoId'
when 'stormwater_structure' then 'systemKey'
when 'stormwater_harvesting_connection' then 'systemKey'
when 'sewer_limit_annotation' then 'rwoId'
when 'critical_customer_location' then 'rwoId'
when 'unsewered_area' then 'id'
when 'billable_loc_type_dyn_enum' then 'billableLocationType'
when 'stormwater_channel_extent' then 'rwoId'
when 'system_area_topo_line' then 'rwoId'
when 'error_log_users' then 'userid'
when 'asset_cof_score_archive' then 'id'
when 'intersection_location' then 'rwoId'
when 'lot_coverage' then 'systemKey'
when 'maintenance_area_annotation' then 'rwoId'
when 'fm_ground_hardstand_area' then 'systemKey'
when 'sewer_environment_condition' then 'systemKey'
when 'iff_alias_text' then 'id'
when 'sp_polygon_coverage' then 'rwoId'
when 'new_asset_transaction' then 'id'
when 'miscellaneous_plan' then 'systemKey'
when 'stormwater_type_annotation' then 'rwoId'
when 'easement_name_list' then 'alias2,alias1'
when 'sewer_pumping_station' then 'systemKey'
when 'aquatic_name_annotation' then 'rwoId'
when 'watermain' then 'systemKey'
when 'rural_water_area' then 'systemKey'
when 'sewer_property_location' then 'rwoId'
when 'lot_coverage_errors' then 'lotCoverageKey'
when 'sewer_cctv_detail' then 'systemKey'
when 'land_development_area' then 'systemKey'
when 'fm_ground_garden_area' then 'systemKey'
when 'historical_dsp_annotation' then 'rwoId'
when 'sewer_lp_cable_location' then 'rwoId'
when 'critical_information' then 'systemKey'
when 'system_area_annotation' then 'rwoId'
when 'maintenance_area' then 'systemKey'
when 'unsewered_area_annotation' then 'rwoId'
when 'zoning_coverage' then 'rwoId'
when 'road_qualifier2_on_centreline' then 'rwoId'
when 'fm_ground_tree_canopy' then 'systemKey'
when 'census_area' then 'systemKey'
when 'sp_polygon_annotation' then 'rwoId'
when 'contact_class_catalogue' then 'systemKey'
when 'stormwater_rehabilitation_detail' then 'systemKey'
when 'landscape_soil_coverage' then 'rwoId'
when 'drawing_function' then 'funcId'
when 'sewer_limit_coverage' then 'rwoId'
when 'suburb' then 'systemKey'
when 'sewer_mining_connection' then 'systemKey'
when 'sp_manual_assigned_asset' then 'assetNumber'
when 'watermain_maintenance' then 'systemKey'
when 'census_area_coverage' then 'rwoId'
when 'maintenance_area_coverage' then 'rwoId'
when 'sewer_plan_number_annotation' then 'rwoId'
when 'govt_land_housing_annotation' then 'rwoId'
when 'linked_document_type' then 'documentType'
when 'waterway_tidal_symbol' then 'rwoId'
when 'mp_location' then 'systemKey'
when 'watermain_main_type_annotation' then 'rwoId'
when 'future_dsp_annotation' then 'rwoId'
when 'pseudo_sewer_main' then 'systemKey'
when 'stormwater_bridge' then 'systemKey'
when 'sewer_valve' then 'systemKey'
when 'water_pollution_control_plant' then 'systemKey'
when 'corporate_property' then 'systemKey'
when 'aquatic_coverage' then 'rwoId'
when 'water_electrical_cable_route' then 'rwoId'
when 'road_qualifier1_on_centreline' then 'rwoId'
when 'aquatic' then 'name'
when 'major_works_area' then 'systemKey'
when 'urban_development_parcel' then 'systemKey'
when 'water_sample_point' then 'systemKey'
when 'stormwater_fitting' then 'systemKey'
when 'sampling_point' then 'systemKey'
when 'landscape_soil_types_catalogue' then 'systemKey'
when 'function_geometry' then 'id'
when 'projection_param' then 'id'
when 'critical_information_catalogue' then 'systemKey'
when 'future_dsp' then 'systemKey'
when 'street_directory_catalogue' then 'systemKey'
when 'sewer_function_annotation' then 'rwoId'
when 'building' then 'systemKey'
when 'easement_segment' then 'systemKey'
when 'govt_land_housing' then 'id'
when 'iworcs_construction_project' then 'systemKey'
when 'pseudo_isection' then 'shutdownNumber'
when 'ubd_grid' then 'edition,map,column,row'
when 'water_calibration_point' then 'systemKey'
when 'sewer_special_annotation' then 'systemKey'
when 'water_owner_catalogue' then 'owner'
when 'easement_width_annotation' then 'rwoId'
when 'cadastral_line' then 'systemKey'
when 'trunk_main_annotation' then 'rwoId'
when 'building_fabric_catalogue' then 'buildingFabric'
when 'sp_topo_line' then 'rwoId'
when 'ground_maintenance' then 'systemKey'
when 'system_area' then 'systemKey'
when 'corporate_realty_easement_id' then 'systemKey'
when 'fm_ground_access_road' then 'systemKey'
when 'sewer_fitting' then 'systemKey'
when 'sewer_ocu_type_catalogue' then 'type'
when 'special_tariff_area_coverage' then 'rwoId'
when 'facility_water_valve' then 'systemKey'
when 'water_joint_type_catalogue' then 'jointType'
when 'easement_name1_annotation' then 'rwoId'
when 'stormwater_channel' then 'systemKey'
when 'iff_dict_text' then 'id'
when 'heritage_name_annotation' then 'rwoId'
when 'water_hydrant' then 'systemKey'
when 'lot_parcel' then 'systemKey'
when 'water_pumping_station' then 'systemKey'
end
where systemCode = '{SYSTEM_CODE}'
""")


# COMMAND ----------


