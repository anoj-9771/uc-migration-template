# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

import json

# COMMAND ----------

j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
environment = [x['value'] for x in j if x['key'] == 'Environment'][0]

# COMMAND ----------

swirldata_tables =['BMS_0002185_849',
'BMS_0002185_850',
'BMS_0002185_851',
'BMS_0002185_852',
'BMS_0002185_853',
'BMS_0002185_854',
'BMS_0002185_855',
'BMS_0002185_856',
'BMS_0002185_857',
'BMS_0002185_858',
'BMS_0002185_859',
'BMS_0002185_860',
'BMS_0002185_861',
'BMS_0002185_862',
'BMS_0002185_863',
'BMS_0002185_865',
'BMS_9999999_1',
'BMS_9999999_100',
'BMS_9999999_101',
'BMS_9999999_103',
'BMS_9999999_104',
'BMS_9999999_108',
'BMS_9999999_109',
'BMS_9999999_110',
'BMS_9999999_111',
'BMS_9999999_112',
'BMS_9999999_115',
'BMS_9999999_134',
'BMS_9999999_135',
'BMS_9999999_136',
'BMS_9999999_137',
'BMS_9999999_138',
'BMS_9999999_139',
'BMS_9999999_140',
'BMS_9999999_141',
'BMS_9999999_142',
'BMS_9999999_144',
'BMS_9999999_146',
'BMS_9999999_147',
'BMS_9999999_148',
'BMS_9999999_149',
'BMS_9999999_152',
'BMS_9999999_153',
'BMS_9999999_154',
'BMS_9999999_155',
'BMS_9999999_156',
'BMS_9999999_157',
'BMS_9999999_158',
'BMS_9999999_159',
'BMS_9999999_161',
'BMS_9999999_169',
'BMS_9999999_170',
'BMS_9999999_174',
'BMS_9999999_175',
'BMS_9999999_176',
'BMS_9999999_177',
'BMS_9999999_178',
'BMS_9999999_180',
'BMS_9999999_182',
'BMS_9999999_183',
'BMS_9999999_186',
'BMS_9999999_188',
'BMS_9999999_190',
'BMS_9999999_191',
'BMS_9999999_192',
'BMS_9999999_193',
'BMS_9999999_197',
'BMS_9999999_198',
'BMS_9999999_199',
'BMS_9999999_200',
'BMS_9999999_201',
'BMS_9999999_202',
'BMS_9999999_203',
'BMS_9999999_204',
'BMS_9999999_205',
'BMS_9999999_206',
'BMS_9999999_208',
'BMS_9999999_210',
'BMS_9999999_212',
'BMS_9999999_214',
'BMS_9999999_216',
'BMS_9999999_218',
'BMS_9999999_219',
'BMS_9999999_220',
'BMS_9999999_221',
'BMS_9999999_223',
'BMS_9999999_224',
'BMS_9999999_225',
'BMS_9999999_226',
'BMS_9999999_227',
'BMS_9999999_229',
'BMS_9999999_230',
'BMS_9999999_231',
'BMS_9999999_232',
'BMS_9999999_234',
'BMS_9999999_235',
'BMS_9999999_236',
'BMS_9999999_238',
'BMS_9999999_239',
'BMS_9999999_240',
'BMS_9999999_241',
'BMS_9999999_242',
'BMS_9999999_500',
'BMS_9999999_501',
'BMS_9999999_502',
'BMS_9999999_503',
'BMS_9999999_504',
'BMS_9999999_506',
'BMS_9999999_507',
'BMS_9999999_508',
'BMS_9999999_750',
'BMS_9999999_751',
'BMS_9999999_752',
'BMS_9999999_800',
'BMS_9999999_801',
'BMS_9999999_802',
'BMS_9999999_803',
'BMS_9999999_804',
'BMS_9999999_805',
'BMS_9999999_806',
'BMS_9999999_807',
'BMS_9999999_808',
'BMS_9999999_809',
'BMS_9999999_810',
'BMS_9999999_811',
'BMS_9999999_812',
'BMS_9999999_813',
'BMS_9999999_814',
'BMS_9999999_815',
'BMS_9999999_816',
'BMS_9999999_817',
'BMS_9999999_818',
'BMS_9999999_819',
'BMS_9999999_820',
'BMS_9999999_821',
'BMS_9999999_822',
'BMS_9999999_823',
'BMS_9999999_824',
'BMS_9999999_825',
'BMS_9999999_826',
'BMS_9999999_827',
'BMS_9999999_828',
'BMS_9999999_829',
'BMS_9999999_830',
'BMS_9999999_831',
'BMS_9999999_832',
'BMS_9999999_833',
'BMS_9999999_834',
'BMS_9999999_835',
'BMS_9999999_836',
'BMS_9999999_837',
'BMS_9999999_838',
'BMS_9999999_839',
'BMS_9999999_840',
'BMS_9999999_841',
'BMS_9999999_842',
'BMS_9999999_843',
'BMS_9999999_844',
'BMS_9999999_846',
'BMS_9999999_90',
'BMS_9999999_91',
'BMS_9999999_108_T9999999_102',
'BMS_9999999_108_T9999999_120',
'BMS_9999999_108_T9999999_121',
'BMS_9999999_108_T9999999_122',
'BMS_9999999_108_T9999999_123',
'BMS_9999999_108_T9999999_125',
'BMS_9999999_108_T9999999_126',
'BMS_9999999_108_T9999999_127',
'BMS_9999999_108_T9999999_129',
'BMS_9999999_108_T9999999_130',
'BMS_9999999_108_T9999999_131',
'BMS_9999999_111_T9999999_101',
'BMS_9999999_111_T9999999_105',
'BMS_9999999_111_T9999999_106',
'BMS_9999999_111_T9999999_107',
'BMS_9999999_111_T9999999_108',
'BMS_9999999_111_T9999999_110',
'BMS_9999999_112_T9999999_101',
'BMS_9999999_112_T9999999_105',
'BMS_9999999_115_T9999999_102',
'BMS_9999999_115_T9999999_103',
'BMS_9999999_115_T9999999_104',
'BMS_9999999_115_T9999999_105',
'BMS_9999999_115_T9999999_106',
'BMS_9999999_115_T9999999_107',
'BMS_9999999_115_T9999999_108',
'BMS_9999999_115_T9999999_109',
'BMS_9999999_115_T9999999_110',
'BMS_9999999_115_T9999999_111',
'BMS_9999999_115_T9999999_112',
'BMS_9999999_115_T9999999_113',
'BMS_9999999_115_T9999999_114',
'BMS_9999999_115_T9999999_115',
'BMS_9999999_115_T9999999_116',
'BMS_9999999_115_T9999999_117',
'BMS_9999999_115_T9999999_120',
'BMS_9999999_115_T9999999_121',
'BMS_9999999_115_T9999999_122',
'BMS_9999999_115_T9999999_123',
'BMS_9999999_115_T9999999_124',
'BMS_9999999_115_T9999999_125',
'BMS_9999999_115_T9999999_126',
'BMS_9999999_115_T9999999_127',
'BMS_9999999_115_T9999999_128',
'BMS_9999999_115_T9999999_129',
'BMS_9999999_115_T9999999_130',
'BMS_9999999_115_T9999999_131',
'BMS_9999999_138_T9999999_119',
'BMS_9999999_158_T9999999_101',
'BMS_9999999_158_T9999999_104',
'BMS_9999999_158_T9999999_106',
'BMS_9999999_158_T9999999_108',
'BMS_9999999_158_T9999999_110',
'BMS_9999999_158_T9999999_112',
'BMS_9999999_190_T9999999_102',
'BMS_9999999_190_T9999999_104',
'BMS_9999999_190_T9999999_106',
'BMS_9999999_190_T9999999_108',
'BMS_9999999_750_T9999999_101',
'BMS_9999999_750_T9999999_102',
'BMS_9999999_750_T9999999_103']

swirlref_tables = ['BMS_LOOKUP_ITEMS',
                   'BMS_MULTILOOKUPITEMS',
                   'BMS_NFCOMPONENTS',
                   'BMS_RELATIONSHIPS',
                   'BMS_COMPONENT_LINKS',
                   'BMS_LOOKUPS']

# COMMAND ----------

group_names = {"swirlref":swirlref_tables
              ,"swirldata":swirldata_tables
              } 
source_key_vault_secret = "daf-oracle-swirl-connectionstring"
source_handler = 'oracle-load'
raw_handler = 'raw-load-delta'
cleansed_handler = 'cleansed-load-delta'
extended_properties = '{"RawTableNameMatchSource" : "True"}'
#extended_properties = '{"RawTableNameMatchSource" : "True", "GroupOrderBy" : "_DLRawZoneTimeStamp Desc"}'

# COMMAND ----------

if environment == 'PREPROD':
    source_schema = "cintel_stg"
else:
    source_schema = "cintel"

# COMMAND ----------

# ------------- CONSTRUCT QUERY ----------------- #
for code in group_names:
    base_query = f"""
    WITH _Base AS 
    (
    SELECT "{code}" SystemCode, "{source_schema}" SourceSchema, "{source_key_vault_secret}" SourceKeyVaultSecret, '' SourceQuery, "{source_handler}" SourceHandler, '' RawFileExtension, "{raw_handler}" RawHandler, '{extended_properties}' ExtendedProperties, "{cleansed_handler}" CleansedHandler, '' WatermarkColumn
    )"""
    select_list = [
        'SELECT "' + x + '" SourceTableName, * FROM _Base' for x in group_names[code]
    ]
    select_string = " UNION ".join(select_list)
    select_statement = select_string + " ORDER BY SourceSchema, SourceTableName"

    df = spark.sql(f"""{base_query} {select_statement}""")
    SYSTEM_CODE = code
        
    AddIngestion(df)

# COMMAND ----------

 # Update Destination Table Name
 ExecuteStatement("""
 UPDATE dbo.extractLoadManifest SET
 DestinationTableName = CASE SourceTableName
 WHEN 'BMS_9999999_109' THEN 'action'
 WHEN 'BMS_9999999_103' THEN 'area'
 WHEN 'BMS_9999999_142' THEN 'aspect'
 WHEN 'BMS_9999999_838' THEN 'asset_information'
 WHEN 'BMS_9999999_806' THEN 'audit'
 WHEN 'BMS_9999999_808' THEN 'audit_answer'
 WHEN 'BMS_9999999_807' THEN 'audit_question'
 WHEN 'BMS_9999999_809' THEN 'audit_template'
 WHEN 'BMS_9999999_805' THEN 'audit_type'
 WHEN 'BMS_9999999_174' THEN 'budget'
 WHEN 'BMS_9999999_205' THEN 'business_objective'
 WHEN 'BMS_9999999_199' THEN 'business_plan'
 WHEN 'BMS_9999999_804' THEN 'change_audit'
 WHEN 'BMS_9999999_801' THEN 'change_implementation_plan'
 WHEN 'BMS_9999999_800' THEN 'change_request'
 WHEN 'BMS_9999999_802' THEN 'change_sme_review'
 WHEN 'BMS_9999999_188' THEN 'communication_log'
 WHEN 'BMS_9999999_192' THEN 'company'
 WHEN 'BMS_9999999_229' THEN 'compliance_event'
 WHEN 'BMS_9999999_231' THEN 'compliance_event_recommendation'
 WHEN 'BMS_9999999_230' THEN 'compliance_event_result'
 WHEN 'BMS_9999999_210' THEN 'condition'
 WHEN 'BMS_9999999_815' THEN 'contact'
 WHEN 'BMS_9999999_818' THEN 'contractor_involved'
 WHEN 'BMS_9999999_823' THEN 'contributing_factors'
 WHEN 'BMS_9999999_149' THEN 'control_measure'
 WHEN 'BMS_9999999_191' THEN 'correspondence_log'
 WHEN 'BMS_9999999_821' THEN 'debrief'
 WHEN 'BMS_9999999_110' THEN 'department'
 WHEN 'BMS_9999999_135' THEN 'document'
 WHEN 'BMS_9999999_182' THEN 'emergency_response'
 WHEN 'BMS_9999999_197' THEN 'equipment'
 WHEN 'BMS_9999999_227' THEN 'exposure_group'
 WHEN 'BMS_9999999_147' THEN 'exposure_standard'
 WHEN 'BMS_9999999_816' THEN 'external_reference'
 WHEN 'BMS_9999999_100' THEN 'file'
 WHEN 'BMS_9999999_813' THEN 'findings'
 WHEN 'BMS_9999999_502' THEN 'ghg_algorithm_definition'
 WHEN 'BMS_9999999_503' THEN 'ghg_algorithm_values'
 WHEN 'BMS_9999999_504' THEN 'ghg_constants'
 WHEN 'BMS_9999999_501' THEN 'ghg_emissions_capture'
 WHEN 'BMS_9999999_508' THEN 'ghg_equipment_history'
 WHEN 'BMS_9999999_506' THEN 'ghg_equipment_type'
 WHEN 'BMS_9999999_500' THEN 'ghg_indicators'
 WHEN 'BMS_9999999_507' THEN 'ghg_parameters'
 WHEN 'BMS_9999999_224' THEN 'hazard_assessment'
 WHEN 'BMS_9999999_223' THEN 'hazard_job_task'
 WHEN 'BMS_9999999_226' THEN 'hazard_notification'
 WHEN 'BMS_9999999_136' THEN 'hazard_register'
 WHEN 'BMS_9999999_225' THEN 'hazard_review'
 WHEN 'BMS_9999999_236' THEN 'health_test'
 WHEN 'BMS_9999999_115' THEN 'health_test_type'
 WHEN 'BMS_9999999_152' THEN 'hours'
 WHEN 'BMS_9999999_178' THEN 'impact'
 WHEN 'BMS_9999999_104' THEN 'incident'
 WHEN 'BMS_9999999_830' THEN 'incident_bypass_and_partial_treatment'
 WHEN 'BMS_9999999_831' THEN 'incident_bypass_external_notification'
 WHEN 'BMS_9999999_108' THEN 'incident_consequence'
 WHEN 'BMS_9999999_825' THEN 'incident_environment'
 WHEN 'BMS_9999999_826' THEN 'incident_environment_impact'
 WHEN 'BMS_9999999_828' THEN 'incident_environment_requirements_not_met'
 WHEN 'BMS_9999999_817' THEN 'incident_impact'
 WHEN 'BMS_0002185_860' THEN 'incident_motor_vehicle_accident'
 WHEN 'BMS_9999999_836' THEN 'incident_network'
 WHEN 'BMS_9999999_827' THEN 'incident_period'
 WHEN 'BMS_0002185_850' THEN 'incident_security'
 WHEN 'BMS_0002185_854' THEN 'incident_security_assault'
 WHEN 'BMS_0002185_851' THEN 'incident_security_break_and_enter'
 WHEN 'BMS_0002185_857' THEN 'incident_security_graffiti'
 WHEN 'BMS_0002185_858' THEN 'incident_security_high_level_security_response'
 WHEN 'BMS_0002185_853' THEN 'incident_security_malicious_damage'
 WHEN 'BMS_0002185_859' THEN 'incident_security_security_equipment_performance'
 WHEN 'BMS_0002185_856' THEN 'incident_security_suspicious_activity'
 WHEN 'BMS_0002185_852' THEN 'incident_security_theft'
 WHEN 'BMS_0002185_855' THEN 'incident_security_trespass'
 WHEN 'BMS_0002185_861' THEN 'incident_vehicles_involved'
 WHEN 'BMS_0002185_863' THEN 'incident_vehicles_involved_infringement_details'
 WHEN 'BMS_0002185_862' THEN 'incident_vehicles_involved_passenger_details'
 WHEN 'BMS_9999999_832' THEN 'incident_water_quality'
 WHEN 'BMS_9999999_834' THEN 'incident_water_quality_data'
 WHEN 'BMS_9999999_835' THEN 'incident_water_quality_data_parameters'
 WHEN 'BMS_9999999_833' THEN 'incident_water_quality_update_comments'
 WHEN 'BMS_9999999_161' THEN 'injury'
 WHEN 'BMS_0002185_849' THEN 'injury_details'
 WHEN 'BMS_9999999_238' THEN 'injury_management'
 WHEN 'BMS_9999999_158' THEN 'injury_management_type'
 WHEN 'BMS_9999999_154' THEN 'injury_number_of_days_lost'
 WHEN 'BMS_9999999_240' THEN 'inspection_and_audit'
 WHEN 'BMS_9999999_111' THEN 'inspection_type'
 WHEN 'BMS_9999999_239' THEN 'investigation'
 WHEN 'BMS_9999999_814' THEN 'keywords'
 WHEN 'BMS_9999999_812' THEN 'lessons_learned'
 WHEN 'BMS_9999999_843' THEN 'licence'
 WHEN 'BMS_9999999_140' THEN 'licence_and_permit'
 WHEN 'BMS_0002185_865' THEN 'licence_clause'
 WHEN 'BMS_9999999_824' THEN 'licence_noncompliance'
 WHEN 'BMS_9999999_839' THEN 'maximo_work_order_details'
 WHEN 'BMS_9999999_144' THEN 'monitoring'
 WHEN 'BMS_9999999_193' THEN 'monitoring_plan'
 WHEN 'BMS_9999999_198' THEN 'monitoring_point'
 WHEN 'BMS_9999999_169' THEN 'monitoring_result'
 WHEN 'BMS_9999999_170' THEN 'monitoring_standard'
 WHEN 'BMS_9999999_819' THEN 'notification_log'
 WHEN 'BMS_9999999_820' THEN 'notification_receiver'
 WHEN 'BMS_9999999_141' THEN 'objective'
 WHEN 'BMS_9999999_159' THEN 'occupation_history'
 WHEN 'BMS_9999999_208' THEN 'participant'
 WHEN 'BMS_9999999_101' THEN 'person'
 WHEN 'BMS_9999999_212' THEN 'person_trained'
 WHEN 'BMS_9999999_216' THEN 'position'
 WHEN 'BMS_9999999_842' THEN 'pre_treatment'
 WHEN 'BMS_9999999_183' THEN 'process'
 WHEN 'BMS_9999999_232' THEN 'project'
 WHEN 'BMS_9999999_139' THEN 'qualification'
 WHEN 'BMS_9999999_218' THEN 'qualification_term'
 WHEN 'BMS_9999999_829' THEN 'regulatory_notice_received'
 WHEN 'BMS_9999999_146' THEN 'rehabilitation'
 WHEN 'BMS_9999999_203' THEN 'risk_assessment'
 WHEN 'BMS_9999999_202' THEN 'risk_register'
 WHEN 'BMS_9999999_175' THEN 'risk_review'
 WHEN 'BMS_9999999_841' THEN 'role_term'
 WHEN 'BMS_9999999_840' THEN 'role_type'
 WHEN 'BMS_9999999_822' THEN 'root_cause_analysis'
 WHEN 'BMS_9999999_241' THEN 'safety_meeting'
 WHEN 'BMS_9999999_112' THEN 'safety_meeting_type'
 WHEN 'BMS_9999999_155' THEN 'sample_result'
 WHEN 'BMS_9999999_242' THEN 'sampling'
 WHEN 'BMS_9999999_138' THEN 'sampling_type'
 WHEN 'BMS_9999999_180' THEN 'stakeholder'
 WHEN 'BMS_9999999_837' THEN 'stakeholder_notification'
 WHEN 'BMS_9999999_177' THEN 'status_update'
 WHEN 'BMS_9999999_201' THEN 'strategy'
 WHEN 'BMS_9999999_204' THEN 'strategy_assessment'
 WHEN 'BMS_9999999_200' THEN 'swot'
 WHEN 'BMS_9999999_846' THEN 'system'
 WHEN 'BMS_9999999_90' THEN 'system_analytics_rules'
 WHEN 'BMS_9999999_91' THEN 'system_analytics_rules_locator'
 WHEN 'BMS_9999999_234' THEN 'system_global_reassignments'
 WHEN 'BMS_9999999_235' THEN 'system_global_reassignments_types'
 WHEN 'BMS_9999999_1' THEN 'system_queue'
 WHEN 'BMS_9999999_810' THEN 'system_report_event_configuration'
 WHEN 'BMS_9999999_811' THEN 'system_report_event_request'
 WHEN 'BMS_9999999_153' THEN 'system_time_period'
 WHEN 'BMS_9999999_186' THEN 'target'
 WHEN 'BMS_9999999_134' THEN 'task_or_activity'
 WHEN 'BMS_9999999_220' THEN 'test_component_lookup'
 WHEN 'BMS_9999999_219' THEN 'test_fields'
 WHEN 'BMS_9999999_221' THEN 'test_tree'
 WHEN 'BMS_9999999_148' THEN 'training_course'
 WHEN 'BMS_9999999_214' THEN 'training_provider'
 WHEN 'BMS_9999999_156' THEN 'training_session'
 WHEN 'BMS_9999999_206' THEN 'treatment_plan'
 WHEN 'BMS_9999999_176' THEN 'waste_management'
 WHEN 'BMS_9999999_751' THEN 'waste_management_disposal'
 WHEN 'BMS_9999999_752' THEN 'waste_management_tracking'
 WHEN 'BMS_9999999_750' THEN 'waste_management_type'
 WHEN 'BMS_9999999_844' THEN 'water_source'
 WHEN 'BMS_9999999_137' THEN 'workers_compensation'
 WHEN 'BMS_9999999_190' THEN 'workers_compensation_claim_management'
 WHEN 'BMS_9999999_157' THEN 'workers_compensation_cost'
 WHEN 'BMS_9999999_803' THEN 'location'
 WHEN 'BMS_LOOKUP_ITEMS' THEN 'lookup_items'
 WHEN 'BMS_MULTILOOKUPITEMS' THEN 'multi_lookup_items'
 WHEN 'BMS_NFCOMPONENTS' THEN 'nf_components'
 WHEN 'BMS_RELATIONSHIPS' THEN 'relationships'
 WHEN 'BMS_COMPONENT_LINKS' THEN 'componentLinks'
 WHEN 'BMS_LOOKUPS' THEN 'lookups'
 ELSE SourceTableName
 END
 WHERE systemCode IN ('swirldata','swirlref')
 """)

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET DestinationSchema = 'swirl' 
    WHERE (SystemCode in ('swirldata','swirlref'))""")

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET BusinessKeyColumn = CASE 
                            WHEN DestinationTableName = 'lookup_items' THEN 'lookupid,lookupitemid'
                            WHEN DestinationTableName = 'multi_lookup_items' THEN 'componentEntityId,componentFieldId,lookupItemId'
                            WHEN DestinationTableName = 'nf_components' THEN 'componentId'
                            WHEN DestinationTableName = 'relationships' THEN 'relationshipId'
                            WHEN DestinationTableName = 'componentLinks' THEN 'fromId,relationshipId,toId'
                            WHEN DestinationTableName = 'lookups' THEN 'lookupId'
                            ELSE 'id'
                            END
    WHERE SystemCode in ('swirldata','swirlref')
    """)

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET ExtendedProperties = '{"RawTableNameMatchSource":"True","CleansedQuery":"SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY bms_id ORDER BY  _DLRawZoneTimeStamp DESC) rn FROM {tableFqn} WHERE _DLRawZoneTimeStamp > ''{lastLoadTimeStamp}'') WHERE rn=1"}'
    WHERE (SystemCode in ('swirldata'))""")
