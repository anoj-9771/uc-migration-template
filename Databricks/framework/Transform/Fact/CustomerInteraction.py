# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

# %sql
# select distinct a.InteractionID, businessPartnerNumber as ReportedByBPNumber,firstName,lastName,mainPartnerFlag from 
# (select I.InteractionID, L.HIGUID,L.setGUID,P.partnerFunctionDescription, P.partnerFunctionCode,  P.mainPartnerFlag ,bptnr.*  from ppd_cleansed.crm.`0crm_sales_act_1` I  
# LEFT JOIN ppd_cleansed.crm.crmd_link L on I.interactionGUID = L.HIGUID
# LEFT JOIN ppd_cleansed.crm.crmd_partner P on L.setGUID = GUID
# LEFT JOIN ppd_cleansed.crm.`0bpartner_attr` AT on AT.businessPartnerGUID = P.partnerNumber
# LEFT JOIN ppd_curated.dim.businessPartner bptnr on AT.businessPartnerNumber = bptnr.businessPartnerNumber
# WHERE P.partnerFunctionCode = 'ICAGENT' and bptnr._recordCurrent = 1 ) a
# INNER join 
# (select InteractionID, count(1) from (select I.InteractionID, L.HIGUID,L.setGUID,P.partnerFunctionDescription, P.partnerFunctionCode,bptnr.*  from ppd_cleansed.crm.`0crm_sales_act_1` I  
# LEFT JOIN ppd_cleansed.crm.crmd_link L on I.interactionGUID = L.HIGUID
# LEFT JOIN ppd_cleansed.crm.crmd_partner P on L.setGUID = GUID
# LEFT JOIN ppd_cleansed.crm.`0bpartner_attr` AT on AT.businessPartnerGUID = P.partnerNumber
# LEFT JOIN ppd_curated.dim.businessPartner bptnr on AT.businessPartnerNumber = bptnr.businessPartnerNumber
# WHERE P.partnerFunctionCode = 'ICAGENT' and bptnr._recordCurrent = 1 ) group by all having count(1)> 1) b on a.InteractionID = b.InteractionID

# COMMAND ----------

#####Determine Load #################
###############################
driverTable1 = 'cleansed.crm.0crm_sales_act_1'   

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}").withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True 
    #derivedDF1 = spark.sql("""select * from ppd_cleansed.crm.0crm_sales_act_1 where to_date(_DLCleansedZoneTimeStamp) = to_date(current_timestamp())""") 
    derivedDF1 = getSourceCDF(driverTable1, None, False)    
    if derivedDF1.count() == 0:
        print("No delta to be  processed")
        #dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1}  -- Destination {_.Destination}") 

# COMMAND ----------

###Variables ############################
dummySK = '60e35f602481e8c37d48f6a3e3d7c30d' ##Derived by hashing -1 and recordStart
#####-----------DIRECT DATAFRAMES CRM--------------------------###############
coreDF = (( derivedDF1 #GetTable(f"{getEnv()}cleansed.crm.0crm_sales_act_1")
        .withColumn("processTypeBK", concat(col("ProcessTypeCode"),lit("|"),lit("CRM")))
        .withColumn("statusBK", concat(col("statusProfile"),lit("|"),col("statusCode")))
        .withColumn("channelBK",concat(trim(col("communicationChannelCode")),lit("|"),lit("CRM")))
        .withColumn("propertyNoBK",when(col("propertyNumber").isNull(), lit('-1'))
                      .otherwise(col("propertyNumber"))))
        .select(col("interactionId")
               ,col("interactionGUID")
               ,col("externalNumber")
               ,col("description")
               ,col("createdDate")
               ,col("priority")
               ,col("direction")
               ,col("directionCode")
               ,col("processTypeBK")
               ,col("statusBK")
               ,col("channelBK")
               ,col("propertyNoBK").cast("long")
               ,col("_change_type")
              ) 
        )

            
crmActDF =  (GetTable(f"{getEnv()}cleansed.crm.crmd_dhr_activ")
                  .select(col("activityGUID") 
                          ,col("categoryLevel1GUID")  
                          ,col("categoryLevel2GUID")
                          ,col("responsibleEmployeeNumber").alias("responsibleEmployeeNumber")
                          ,col("activityPartnerNumber").cast("long").alias("propertyNumber")
                         )
            )

crmCatDF =  (GetTable(f"{getEnv()}cleansed.crm.crmc_erms_cat_ct")
                 .select(col("categoryGUID") 
                         ,col("categoryDescription").alias("customerInteractionCategory")
                         ,col("categoryDescription").alias("customerInterationSubCategory")
                        )
            )

crmLinkDF = (GetTable(f"{getEnv()}cleansed.crm.crmd_link")
                           .select( col("hiGUID")
                                   ,col("setGUID")
                                   ,col("setObjectType"))
            )

crmPartnerDF = (GetTable(f"{getEnv()}cleansed.crm.crmd_partner")
                            .select(col("GUID")
                                    ,col("partnerNumber")
                                    ,col("partnerFunctionCode")
                                    ,col("mainPartnerFlag")).filter("partnerFunctionCode in ('ICAGENT','ZCONTPER','ZRPRTDBY','00000056')").drop_duplicates()
            )

crm0bpartner_attrDF = (GetTable(f"{getEnv()}cleansed.crm.0bpartner_attr")
                            .select(col("businessPartnerGUID")
                                    ,col("businessPartnerNumber")).drop_duplicates()
    )
    

crmJcdsDF = spark.sql(f""" 
                select objectGUID, endDate from 
(select objectGUID,to_timestamp(concat(changeAt,' ',changeTS),'yyyy-MM-dd HHmmss') as endDate, row_number() over(partition by objectGUID order by changeNumber desc) as rownumb from {getEnv()}cleansed.crm.crm_jcds where objectStatus = 'E0003') where rownumb = 1 
                   """
               )

crmOrdIdxDF = (GetTable(f"{getEnv()}cleansed.crm.crmd_order_index")   
                   .filter(to_date(col("ContactEndDatetime")) != to_date(lit('9999-12-31'), 'yyyy-MM-dd'))
                   .select(col("headerGUID")
                          ,col("ContactEndDatetime"))
               )



procTypeDF = ( GetTable(f"{getEnv()}curated.dim.customerserviceprocesstype")
                                          .filter(col("_recordCurrent") == lit("1"))
                           .select( col("customerServiceProcessTypeSK").alias("processTypeFK")
                                     ,col("_BusinessKey")
                                  ) 
             )
    
propertyDF = ( GetTable(f"{getEnv()}curated.dim.Property")
                           .select( col("propertySK").alias("propertyFK")
                                     ,col("_BusinessKey")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")                                  
                              ) 
                 
            )
    
statusDF = ( GetTable(f"{getEnv()}curated.dim.customerinteractionstatus")
                                        .filter(col("_recordCurrent") == lit("1"))
                           .select( col("customerInteractionStatusSK").alias("StatusFK")
                                     ,col("_BusinessKey")
                                   ) 
            )
    
    
channelDF = ( GetTable(f"{getEnv()}curated.dim.CommunicationChannel") 
                     .filter(col("_recordCurrent") == lit("1"))
                     .select( col("communicationChannelSK").alias("communicationChannelFK")
                              ,col("_BusinessKey")
                      ) 
                 
           )

locationDF = ( GetTable(f"{getEnv()}curated.dim.location")   
                   .select(col("locationSK").alias("locationFK"),
                           col("locationID"),                           
                           col("_RecordStart"),
                           col("_RecordEnd"))
            )

busPartDF = ( GetTable(f"{getEnv()}curated.dim.BusinessPartner")
              .withColumn("rank",rank().over(Window.partitionBy("businessPartnerNumber").orderBy(col("sourceSystemCode").desc()))) 
                .filter("rank == 1") 
                             .select( col("businessPartnerSK").alias("responsibleEmployeeFK")
                                     ,col("businessPartnerSK").alias("contactPersonFK")
                                     ,col("businessPartnerSK").alias("serviceTeamFK")
                                     ,col("businessPartnerSK").alias("reportByPersonFK")
                                     ,col("businessPartnerNumber")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")
                      ) 
            )
    
busPartGrpDF = ( GetTable(f"{getEnv()}curated.dim.BusinessPartnerGroup")
                             .select( col("businessPartnerGroupSK").alias("businessPartnerGroupFK")
                                     ,col("businessPartnerGroupNumber")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")
                      ) 
                 
            )

dateDF = (GetTable(f"{getEnv()}curated.dim.Date")
                   .select(col("dateSK").alias("customerInteractionCreatedDateFK")
                          ,col("dateSK").alias("customerInteractionEndDateFK")
                          ,col("calendarDate")
                          )
            )

# COMMAND ----------

resp_partner_df = (crmPartnerDF.filter("partnerFunctionCode = 'ICAGENT'")
 .join(crmLinkDF,col("setGUID") == col("GUID"))
 .join(crm0bpartner_attrDF.select('businessPartnerGUID',col("businessPartnerNumber").alias("employeeResponsibleNumber")),(col("partnerNumber") == col("businessPartnerGUID")), "left").select(col("hiGUID").alias("rp_higuid"),"employeeResponsibleNumber","mainPartnerFlag"))
 
contactPersonNumber_df = (crmPartnerDF.filter("partnerFunctionCode = 'ZCONTPER'")
 .join(crmLinkDF,col("setGUID") == col("GUID"))
 .join(crm0bpartner_attrDF.select('businessPartnerGUID',col("businessPartnerNumber").alias("contactPersonNumber")),(col("partnerNumber") == col("businessPartnerGUID")), "left").select(col("hiGUID").alias("cp_higuid"),"contactPersonNumber","mainPartnerFlag"))
 
serviceTeamNumber_df = (crmPartnerDF.filter("partnerFunctionCode = '00000056'")
 .join(crmLinkDF,col("setGUID") == col("GUID"))
 .join(crm0bpartner_attrDF.select('businessPartnerGUID',col("businessPartnerNumber").alias("serviceTeamNumber")),(col("partnerNumber") == col("businessPartnerGUID")), "left").select(col("hiGUID").alias("st_higuid"),"serviceTeamNumber","mainPartnerFlag"))
 
reportByNumber_df = (crmPartnerDF.filter("partnerFunctionCode = 'ZRPRTDBY'")
 .join(crmLinkDF,col("setGUID") == col("GUID"))
 .join(crm0bpartner_attrDF.select('businessPartnerGUID',col("businessPartnerNumber").alias("reportByNumber")),(col("partnerNumber") == col("businessPartnerGUID")), "left").select(col("hiGUID").alias("rb_higuid"),"reportByNumber","mainPartnerFlag"))

# COMMAND ----------

##############################Main DATAFRAME / Join FOR CRM ###############################
interDF =(coreDF.alias("core")
    .join(resp_partner_df, (col("core.interactionGUID") == col("rp_higuid")), "left")
    .withColumn("rank",rank().over(Window.partitionBy("interactionID").orderBy(col("mainPartnerFlag").desc()))) 
    .filter("rank == 1").drop("rank","mainPartnerFlag")  # Handle duplicates for responsibleEmp 
    
    .join(contactPersonNumber_df,(col("core.interactionGUID") == col("cp_higuid")), "left")
    .withColumn("rank",rank().over(Window.partitionBy("interactionID").orderBy(col("mainPartnerFlag").desc()))) 
    .filter("rank == 1").drop("rank","mainPartnerFlag")  # Handle duplicates for reportBy 
    
    .join(serviceTeamNumber_df,(col("core.interactionGUID") == col("st_higuid")), "left")
    .withColumn("rank",rank().over(Window.partitionBy("interactionID").orderBy(col("mainPartnerFlag").desc()))) \
    .filter("rank == 1").drop("rank","mainPartnerFlag")  # Handle duplicates for reportBy 
    
    .join(reportByNumber_df,(col("core.interactionGUID") == col("rb_higuid")), "left")
    .withColumn("rank",rank().over(Window.partitionBy("interactionID").orderBy(col("mainPartnerFlag").desc()))) 
    .filter("rank == 1").drop("rank","mainPartnerFlag")  # Handle duplicates for reportBy 
    
    .withColumn("employeeResponsibleNumberBK", when(col("employeeResponsibleNumber").isNull(), lit('-1'))
                    .otherwise(regexp_replace(col("employeeResponsibleNumber"), "^0*", ""))) 
    .withColumn("contactPersonNoBK",when(col("contactPersonNumber").isNull(), lit('-1'))
                    .otherwise(regexp_replace(col("contactPersonNumber"), "^0*", "")))
    .withColumn("serviceTeamBK", when(col("serviceTeamNumber").isNull(), lit('-1'))
                    .otherwise(regexp_replace(col("serviceTeamNumber"), "^0*", "")))
    .withColumn("reportByPersonBK", when(col("reportByNumber").isNull(), lit('-1'))
                    .otherwise(regexp_replace(col("reportByNumber"), "^0*", "")))         
)

interDF = ((interDF.alias("inter")
  .join(crmActDF.alias("ca"), (col("inter.interactionGUID") == col("ca.activityGUID")), "left") 

  .join(crmCatDF.alias("cc"), (col("ca.categoryLevel1GUID") == col("cc.categoryGUID")), "left") 
  .drop(col("cc.categoryGUID"),col("cc.customerInterationSubCategory"))
  .join(crmCatDF.alias("cc1"), (col("ca.categoryLevel2GUID") == col("cc1.categoryGUID")), "left") 
  .drop(col("ca.activityGUID"), col("ca.categoryLevel1GUID"), col("ca.categoryLevel2GUID"), 
                          col("cc1.categoryGUID"), col("cc1.customerInteractionCategory")))
  
  .join(propertyDF.alias("pr"), ((col("ca.propertyNumber") == col("pr._BusinessKey")) &  
                 (col("inter.createdDate").between(col("pr._recordStart"), 
                                                          col("pr._recordEnd")))), "left")
  .drop(col("pr._BusinessKey"), col("pr._recordStart"), col("pr._recordEnd"))

  .join(locationDF.alias("lo"), ((col("ca.propertyNumber") == col("lo.locationID")) &  
                 (col("inter.createdDate").between(col("lo._recordStart"), 
                                                          col("lo._recordEnd")))),"left")
  .drop(col("lo.locationID"), col("lo._recordStart"), col("lo._recordEnd"))

  .join(busPartGrpDF.alias("bgp"), ((col("ca.propertyNumber") == col("bgp.businessPartnerGroupNumber")) &  
                 (col("inter.createdDate").between(col("bgp._recordStart"), 
                                                col("bgp._recordEnd")))), "left")
  .drop(col("bgp.businessPartnerGroupNumber"), col("bgp._recordStart"), col("bgp._recordEnd"), col("ca.propertyNumber"))

#   .join(crmLinkDF.filter(col("setObjectType") == lit("30")).alias("cl"), (col("inter.interactionGUID") == col("cl.hiGUID")), "left") 
#   .join(crmSappSegDF.alias("css"), (col("cl.setGUID") == col("css.applicationGUID")), "left")
  .join(crmJcdsDF.alias("css"),(col("inter.interactionGUID") == col("css.objectGUID")), "left")
  .join(dateDF.alias("dt"), (to_date(col("css.endDate")) == col("dt.calendarDate")), "left")
  .drop(col("css.objectGUID"),col("dt.calendarDate"), col("dt.customerInteractionCreatedDateFK"))

  .join(procTypeDF.alias("pc"), (col("inter.processTypeBK") == col("pc._BusinessKey")), "left")
  .drop(col("inter.processTypeBK"),col("pc._BusinessKey")) 

  .join(channelDF.alias("ch"), (col("inter.channelBK") == col("ch._BusinessKey")), "left")
  .drop(col("inter.channelBK"), col("ch._BusinessKey"))

  .join(dateDF.alias("dt1"), (to_date(col("inter.createdDate")) == col("dt1.calendarDate")), "left")
  .drop(col("dt1.calendarDate"), col("dt1.customerInteractionEndDateFK")) 

  .join(busPartDF.select("responsibleEmployeeFK","businessPartnerNumber","_recordStart","_recordEnd").alias("bp"), 
        ((col("inter.employeeResponsibleNumberBK") == col("bp.businessPartnerNumber")) &  
                 (col("inter.createdDate").between(col("bp._recordStart"), 
                                                        col("bp._recordEnd")))), 'left')
  .drop(col("bp.businessPartnerNumber"), col("bp._recordStart"), col("bp._recordEnd"))

  .join(busPartDF.select("contactPersonFK","businessPartnerNumber","_recordStart","_recordEnd").alias("bp1"), 
        ((col("inter.contactPersonNoBK") == col("bp1.businessPartnerNumber")) &  
                 (col("inter.createdDate").between(col("bp1._recordStart"), 
                                                        col("bp1._recordEnd")))), 'left')
  .drop(col("bp1.businessPartnerNumber"), col("bp1._recordStart"), col("bp1._recordEnd"))

  .join(busPartDF.select("reportByPersonFK","businessPartnerNumber","_recordStart","_recordEnd").alias("bp2"), 
        ((col("inter.reportByPersonBK") == col("bp2.businessPartnerNumber")) &  
                 (col("inter.createdDate").between(col("bp2._recordStart"), 
                                                        col("bp2._recordEnd")))), 'left')
  .drop(col("bp2.businessPartnerNumber"), col("bp2._recordStart"), col("bp2._recordEnd"))
  
  .join(busPartDF.select("serviceTeamFK","businessPartnerNumber","_recordStart","_recordEnd").alias("bp3"), 
        ((col("inter.serviceTeamBK") == col("bp3.businessPartnerNumber")) &  
                 (col("inter.createdDate").between(col("bp3._recordStart"), 
                                                        col("bp3._recordEnd")))), 'left')
  .drop(col("bp3.businessPartnerNumber"), col("bp3._recordStart"), col("bp3._recordEnd"))


  .join(statusDF.alias("st"), (col("inter.statusBK") == col("st._BusinessKey")), "left")
  .drop(col("inter.statusBK"), col("st._BusinessKey"))
  
  .select(col("interactionId").alias(f"{BK}")  
         ,col("interactionId").alias("customerInteractionId")
         ,when(col("processTypeFK").isNull(), lit('-1')).otherwise(col("processTypeFK")).alias("customerServiceProcessTypeFK") 
         ,when(col("communicationChannelFK").isNull(), lit('-1')).otherwise(col("communicationChannelFK"))
         .alias("communicationChannelFK")
         ,when(col("statusFK").isNull(), lit('-1')).otherwise(col("statusFK")).alias("customerInteractionStatusFK")
         ,col("responsibleEmployeeFK")
         ,col("serviceTeamFK")
         ,col("contactPersonFK")
         ,col("reportByPersonFK")         
         ,when(col("propertyFK").isNull(), lit(f"{dummySK}")).otherwise(col("propertyFK")).alias("propertyFK")
         ,when(col("locationFK").isNull(), lit(f"{dummySK}")).otherwise(col("locationFK")).alias("locationFK")  
         ,when(col("businessPartnerGroupFK").isNull(), lit(f"{dummySK}")).otherwise(col("businessPartnerGroupFK")).alias("businessPartnerGroupFK")
         ,col("customerInteractionCreatedDateFK")
         ,col("customerInteractionEndDateFK")         
         ,col("customerInteractionCategory")
         ,col("customerInterationSubCategory")
         ,col("interactionGUID").alias("customerInteractionGUID")
         ,col("externalNumber").alias("customerInteractionExternalNumber")
         ,col("description").alias("customerInteractionDescription")
         ,col("createdDate").alias("customerInteractionCreatedTimestamp")
         ,col("endDate").alias("customerInteractionEndDateTimestamp")
         ,col("priority").alias("customerInteractionPriorityIndicator")
         ,col("direction").alias("customerInteractionDirectionIndicator")
         ,col("directionCode").alias("customerInteractionDirectionCode")
         ,col("_change_type")
       )

  )

# COMMAND ----------

if not isDeltaLoad:
    enableCDF(f"{getEnv()}curated.fact.customerinteraction")
    
SaveWithCDF(interDF, 'APPEND')
#Save(interDF, append=True)

# #CleanSelf()
# if isDeltaLoad:
#     SaveWithCDF(interDF, 'APPEND')
# else:
#     SaveWithCDF(interDF, 'APPEND')
#     #enableCDF(_.Destination)

# def Transform():
#     global df
#     df = interDF
#     # ------------- TRANSFORMS ------------- #
#     _.Transforms = ["*"]
#     df = df.selectExpr(_.Transforms)
#     # display(df)
#     #CleanSelf()
#     Save(df)
#     #DisplaySelf()
# Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select customerinteractionSK,count(1) from ppd_curated.fact.customerinteraction group by all having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from ppd_cleansed.crm.0crm_sales_act_1  

# COMMAND ----------


