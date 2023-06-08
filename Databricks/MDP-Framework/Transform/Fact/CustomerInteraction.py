# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

###Variables ############################
dummySK = '60e35f602481e8c37d48f6a3e3d7c30d' ##Derived by hashing -1 and recordStart
#####-----------DIRECT DATAFRAMES CRM--------------------------###############
coreDF = ((GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0crm_sales_act_1')}") 
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
              ) 
        )


crmActDF =  (GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_dhr_activ')}")
                  .select(col("activityGUID") 
                          ,col("categoryLevel1GUID")  
                          ,col("categoryLevel2GUID")
                         )
            )

crmCatDF =  (GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmc_erms_cat_ct')}")
                 .select(col("categoryGUID") 
                         ,col("categoryDescription").alias("customerInteractionCategory")
                         ,col("categoryDescription").alias("customerInterationSubCategory")
                        )
            )

crmLinkDF = ( GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_link')}")
                           .select( col("hiGUID")
                                   ,col("setGUID")).filter(col("setObjectType") == lit("30"))
             
            )
    

crmSappSegDF = (GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_scapptseg')}")
                   .filter(col("apptType") == lit("ORDERPLANNED"))
                   .select(col("applicationGUID")
                          ,col("apptEndDatetime"))
               )


procTypeDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimcustomerserviceprocesstype')}")
                                          .filter(col("_recordCurrent") == lit("1"))
                           .select( col("customerServiceProcessTypeSK").alias("processTypeFK")
                                     ,col("_BusinessKey")
                                  ) 
             )
    
propertyDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimProperty')}")
                           .select( col("propertySK").alias("propertyFK")
                                     ,col("_BusinessKey")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")                                  
                              ) 
                 
            )
    
statusDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimcustomerservicerequestStatus')}")
                                        .filter(col("_recordCurrent") == lit("1"))
                           .select( col("customerServiceRequestStatusSK").alias("StatusFK")
                                     ,col("_BusinessKey")
                                   ) 
            )
    
    
channelDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimCommunicationChannel')}")
                     .filter(col("_recordCurrent") == lit("1"))
                     .select( col("communicationChannelSK").alias("communicationChannelFK")
                              ,col("_BusinessKey")
                      ) 
                 
           )

locationDF = (GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimlocation')}")
                   .select(col("locationSK").alias("locationFK"),
                           col("locationID"),                           
                           col("_RecordStart"),
                           col("_RecordEnd"))
            )

busPartDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimBusinessPartner')}")
                             .select( col("businessPartnerSK").alias("responsibleEmployeeFK")
                                     ,col("businessPartnerSK").alias("contactPersonFK")
                                     ,col("businessPartnerNumber")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")
                      ) 
                 
            )
    
busPartGrpDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimBusinessPartnerGroup')}")
                             .select( col("businessPartnerGroupSK").alias("businessPartnerGroupFK")
                                     ,col("businessPartnerGroupNumber")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")
                      ) 
                 
            )

dateDF = (GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimDate')}")
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
  
  .join(propertyDF.alias("pr"), ((col("core.propertyNoBK") == col("pr._BusinessKey")) &  
                 (col("core.createdDate").between(col("pr._recordStart"), 
                                                          col("pr._recordEnd")))), "left")
  .drop(col("pr._BusinessKey"), col("pr._recordStart"), col("pr._recordEnd"))  

  .join(locationDF.alias("lo"), ((col("core.propertyNoBK") == col("lo.locationID")) &  
                 (col("core.createdDate").between(col("lo._recordStart"), 
                                                          col("lo._recordEnd")))),"left")
  .drop(col("lo.locationID"), col("lo._recordStart"), col("lo._recordEnd")) 

  .join(busPartDF.alias("bp"), 
        ((col("core.employeeResponsibleNumberBK") == col("bp.businessPartnerNumber")) &  
                 (col("core.createdDate").between(col("bp._recordStart"), 
                                                        col("bp._recordEnd")))), 'left')
  .drop(col("bp.businessPartnerNumber"), col("bp._recordStart"), col("bp._recordEnd"), col("bp.contactPersonFK"))

  .join(busPartDF.alias("bp1"), 
        ((col("core.contactPersonNoBK") == col("bp1.businessPartnerNumber")) &  
                 (col("core.createdDate").between(col("bp1._recordStart"), 
                                                        col("bp1._recordEnd")))), 'left')
  .drop(col("bp1.businessPartnerNumber"), col("bp1._recordStart"), col("bp1._recordEnd"), col("bp1.responsibleEmployeeFK"))

  .join(busPartGrpDF.alias("bgp"), ((col("core.propertyNoBK") == col("bgp.businessPartnerGroupNumber")) &  
                 (col("core.createdDate").between(col("bgp._recordStart"), 
                                                col("bgp._recordEnd")))), "left")
  .drop(col("bgp.businessPartnerGroupNumber"), col("bgp._recordStart"), col("bgp._recordEnd"))
 
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
       )

  )

# COMMAND ----------

def Transform():
    global df
    df = interDF
    # ------------- TRANSFORMS ------------- #
    _.Transforms = ["*"]
    df = df.selectExpr(_.Transforms)
    # display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC Create view curated.viewdimbusinesspartnergroupL2 as
# MAGIC
# MAGIC Select  
# MAGIC
# MAGIC businessPartnerGroupSK,
# MAGIC
# MAGIC sourceSystemCode,
# MAGIC
# MAGIC businessPartnerGroupNumber,
# MAGIC
# MAGIC businessPartnerGroupCode,
# MAGIC
# MAGIC businessPartnerCategoryCode,
# MAGIC
# MAGIC businessPartnerCategory,
# MAGIC
# MAGIC businessPartnerTypeCode,
# MAGIC
# MAGIC businessPartnerType,
# MAGIC
# MAGIC externalNumber,
# MAGIC
# MAGIC businessPartnerGUID,
# MAGIC
# MAGIC businessPartnerGroupName1,
# MAGIC
# MAGIC businessPartnerGroupName2,
# MAGIC
# MAGIC consent1Indicator,
# MAGIC
# MAGIC indicatorCreatedUserId,
# MAGIC
# MAGIC indicatorCreatedDate,
# MAGIC
# MAGIC createdBy,
# MAGIC
# MAGIC createdDateTime,
# MAGIC
# MAGIC lastUpdatedBy,
# MAGIC
# MAGIC lastUpdatedDateTime,
# MAGIC
# MAGIC validFromDate,
# MAGIC
# MAGIC validToDate,
# MAGIC
# MAGIC _BusinessKey,
# MAGIC
# MAGIC _DLCuratedZoneTimeStamp,
# MAGIC
# MAGIC _RecordStart,
# MAGIC
# MAGIC _RecordEnd,
# MAGIC
# MAGIC _RecordDeleted,
# MAGIC
# MAGIC _RecordCurrent
# MAGIC
# MAGIC from curated.dimbusinesspartnergroup

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.viewdimbusinesspartnergroupL2
