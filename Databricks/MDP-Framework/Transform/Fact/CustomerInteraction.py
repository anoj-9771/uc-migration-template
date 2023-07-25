# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

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
        .withColumn("employeeResponsibleNumberBK", when(col("employeeResponsibleNumber").isNull(), lit('-1'))
                     .otherwise(regexp_replace(col("employeeResponsibleNumber"), "^0*", ""))) 
        .withColumn("contactPersonNoBK",when(col("contactPersonNumber").isNull(), lit('-1'))
                      .otherwise(regexp_replace(col("contactPersonNumber"), "^0*", "")))
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
               ,col("employeeResponsibleNumberBK")
               ,col("contactPersonNoBK")
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
                                   ,col("setGUID")).filter(col("setObjectType") == lit("30"))
             
            )
    

crmSappSegDF = (GetTable(f"{getEnv()}cleansed.crm.scapptseg")   
                   .filter(col("apptType") == lit("ORDERPLANNED"))
                   .select(col("applicationGUID")
                          ,col("apptEndDatetime"))
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
                             .select( col("businessPartnerSK").alias("responsibleEmployeeFK")
                                     ,col("businessPartnerSK").alias("contactPersonFK")
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

##############################Main DATAFRAME / Join FOR CRM ###############################
interDF = ((coreDF.alias("core")
  .join(crmActDF.alias("ca"), (col("core.interactionGUID") == col("ca.activityGUID")), "left") 

  .join(crmCatDF.alias("cc"), (col("ca.categoryLevel1GUID") == col("cc.categoryGUID")), "left") 
  .drop(col("cc.categoryGUID"),col("cc.customerInterationSubCategory"))
  .join(crmCatDF.alias("cc1"), (col("ca.categoryLevel2GUID") == col("cc1.categoryGUID")), "left") 
  .drop(col("ca.activityGUID"), col("ca.categoryLevel1GUID"), col("ca.categoryLevel2GUID"), 
                          col("cc1.categoryGUID"), col("cc1.customerInteractionCategory")))
  
  .join(busPartDF.alias("bp"), 
        ((col("ca.responsibleEmployeeNumber") == col("bp.businessPartnerNumber")) &  
                 (col("core.createdDate").between(col("bp._recordStart"), 
                                                        col("bp._recordEnd")))), 'left')
  .drop(col("bp.businessPartnerNumber"), col("bp._recordStart"), col("bp._recordEnd"), col("bp.contactPersonFK"), 
         col("ca.responsibleEmployeeNumber"))
           
  .join(propertyDF.alias("pr"), ((col("ca.propertyNumber") == col("pr._BusinessKey")) &  
                 (col("core.createdDate").between(col("pr._recordStart"), 
                                                          col("pr._recordEnd")))), "left")
  .drop(col("pr._BusinessKey"), col("pr._recordStart"), col("pr._recordEnd"))

  .join(locationDF.alias("lo"), ((col("ca.propertyNumber") == col("lo.locationID")) &  
                 (col("core.createdDate").between(col("lo._recordStart"), 
                                                          col("lo._recordEnd")))),"left")
  .drop(col("lo.locationID"), col("lo._recordStart"), col("lo._recordEnd"))

  .join(busPartGrpDF.alias("bgp"), ((col("ca.propertyNumber") == col("bgp.businessPartnerGroupNumber")) &  
                 (col("core.createdDate").between(col("bgp._recordStart"), 
                                                col("bgp._recordEnd")))), "left")
  .drop(col("bgp.businessPartnerGroupNumber"), col("bgp._recordStart"), col("bgp._recordEnd"), col("ca.propertyNumber"))

  .join(crmLinkDF.alias("cl"), (col("core.interactionGUID") == col("cl.hiGUID")), "left") 
  .join(crmSappSegDF.alias("css"), (col("cl.setGUID") == col("css.applicationGUID")), "left")
  .join(dateDF.alias("dt"), (to_date(col("css.apptEndDatetime")) == col("dt.calendarDate")), "left")
  .drop(col("cl.setGUID"), col("cl.hiGUID"), col("css.applicationGUID"),col("dt.calendarDate"), 
        col("dt.customerInteractionCreatedDateFK"))

  .join(procTypeDF.alias("pc"), (col("core.processTypeBK") == col("pc._BusinessKey")), "left")
  .drop(col("core.processTypeBK"),col("pc._BusinessKey")) 

  .join(channelDF.alias("ch"), (col("core.channelBK") == col("ch._BusinessKey")), "left")
  .drop(col("core.channelBK"), col("ch._BusinessKey"))

  .join(dateDF.alias("dt1"), (to_date(col("core.createdDate")) == col("dt1.calendarDate")), "left")
  .drop(col("dt1.calendarDate"), col("dt1.customerInteractionEndDateFK")) 

  .join(busPartDF.alias("bp1"), 
        ((col("core.contactPersonNoBK") == col("bp1.businessPartnerNumber")) &  
                 (col("core.createdDate").between(col("bp1._recordStart"), 
                                                        col("bp1._recordEnd")))), 'left')
  .drop(col("bp1.businessPartnerNumber"), col("bp1._recordStart"), col("bp1._recordEnd"), col("bp1.responsibleEmployeeFK"))

  .join(statusDF.alias("st"), (col("core.statusBK") == col("st._BusinessKey")), "left")
  .drop(col("core.statusBK"), col("st._BusinessKey"))
  
  .select(col("interactionId").alias(f"{BK}")  
         ,col("interactionId").alias("customerInteractionId")
         ,when(col("processTypeFK").isNull(), lit('-1')).otherwise(col("processTypeFK")).alias("customerServiceProcessTypeFK") 
         ,when(col("communicationChannelFK").isNull(), lit('-1')).otherwise(col("communicationChannelFK"))
         .alias("communicationChannelFK")
         ,when(col("statusFK").isNull(), lit('-1')).otherwise(col("statusFK")).alias("customerInteractionStatusFK")
         ,col("responsibleEmployeeFK")
         ,col("contactPersonFK")         
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
         ,col("apptEndDatetime").alias("customerInteractionEndDateTimestamp")
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
