# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    global worknote_df
    global factservicerequest_df
    global crm_crmd_brelvonae_df
    global dimemailheader_df
    global wn_summary_df
    global wn_resolution_df
    global interaction_wn_df
    global email_df
    global interaction_email_df
    global factinteraction_df
    # ------------- TABLES ----------------- #
    worknote_df = GetTable(f"{TARGET}.worknote").alias('WN')
    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')
    factservicerequesta_df = GetTable(f"{TARGET}.factservicerequest").alias('SRA')
    
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')
    dimemailheader_df = GetTable(f"{TARGET}.dimemailheader").alias('H')
    
    factinteraction_df = GetTable(f"{TARGET}.factinteraction").alias('IR')
    
    crm_crmd_link_df = GetTable(f"{SOURCE}.crm_crmd_link").alias('L')
    crm_scapptseg_df = GetTable(f"{SOURCE}.crm_scapptseg").alias('S')
    dimtransactiondate_df = GetTable(f"{TARGET}.dimtransactiondate").alias('T')
    
    crm_skwg_brel_df = GetTable(f"{SOURCE}.crm_skwg_brel").alias('B')
    crm_crmorderphio_df = GetTable(f"{SOURCE}.crm_crmorderphio").alias('O')
    dimattachmentinfo_df = GetTable(f"{TARGET}.dimattachmentinfo").alias('A')
    
                                
    # ------------- JOINS ------------------ #
    wn_summary_df = (
        factservicerequest_df
          .join(worknote_df,expr("SR.serviceRequestID = WN.objectID and SR.changeDateTime >= WN.createdTimeStamp and SR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.workNoteType = 'Summary' and WN.objectTypeCode = 'BUS2000223'")
          .selectExpr("SR.serviceRequestSK as primaryFK","WN.worknoteSK as secondaryFK","'Service Request -Summary Work Note' as relationshipType")
    )
    wn_resolution_df = (
        factservicerequest_df
          .join(worknote_df,expr("SR.serviceRequestID = WN.objectID and SR.changeDateTime >= WN.createdTimeStamp and SR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.workNoteType = 'Resolution' and WN.objectTypeCode = 'BUS2000223'")        
          .selectExpr("SR.serviceRequestSK as primaryFK","WN.worknoteSK as secondaryFK","'Service Request -Resolution Work Note' as relationshipType")    
    )
    interaction_wn_df = (
        factinteraction_df
          .join(worknote_df,expr("IR.interactionID = WN.objectID and IR.createdDate >= CAST(WN.createdTimeStamp AS DATE) and IR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.workNoteType = 'Summary' and WN.objectTypeCode = 'BUS2000126'")            
          .selectExpr("IR.interactionSK as primaryFK","WN.worknoteSK as secondaryFK","'Interaction -Summary Work Note' as relationshipType")    
    )    
    email_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'SOFM'")
          #From Spec - no filter on recordStart nor recordEnd
#           .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID"))
#           .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA"))
          #Will propose - since there doesn't seem to be any date field in B table, use _recordCurrent instead
          .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID and H._recordCurrent = 1"))
          .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA and SR._recordCurrent = 1"))          
          .selectExpr("SR.serviceRequestSK as primaryFK","H.emailHeaderSK as secondaryFK","'Service Request - Email' as relationshipType")    
    )
    interaction_email_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000126' and B.objectTypeB = 'SOFM'")
          #From Spec - no filter on recordStart nor recordEnd
#           .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID"))
#           .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA"))
          #Will propose - since there doesn't seem to be any date field in B table, use _recordCurrent instead
          .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID and H._recordCurrent = 1"))
          .join(factinteraction_df,expr("IR.interactionGUID = B.objectKeyA and IR._recordCurrent = 1"))
          .selectExpr("IR.interactionSK as primaryFK","H.emailHeaderSK as secondaryFK","'Interaction - Email' as relationshipType")    
    )
#     Developer: Prags
    intern_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000126' and B.objectTypeB = 'BUS2000223'") 
          .join(factinteraction_df,expr("IR.interactionGUID = B.objectKeyA"), "Inner") 
          .join(factservicerequest_df,expr("(SR.serviceRequestGUID = B.objectKeyB) and (IR._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("IR.interactionSK as primaryFK","SR.serviceRequestSK as secondaryFK","'Interaction - Service Request' as relationshipType")
    )
    
    servReq_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000223'") 
          .join(factservicerequesta_df,expr("SRA.serviceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factservicerequest_df,expr("(SR.serviceRequestGUID = B.objectKeyB) and (SRA._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("SRA.serviceRequestSK as primaryFK","SR.serviceRequestSK as secondaryFK","'Service Request - Service Request' as relationshipType")
    )
    
    servReq_intern_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000126'") 
          .join(factservicerequesta_df,expr("SRA.serviceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factinteraction_df,expr("(IR.interactionGUID = B.objectKeyB) and (SRA._recordStart between IR._recordStart and IR._recordEnd)"),"Inner")
          .filter(expr("B.objectKeyB <> B.objectKeyA"))
          .selectExpr("SRA.serviceRequestSK as primaryFK","IR.interactionSK as secondaryFK","'Service Request - Interaction' as relationshipType")
    )
    
    servReq_date_df = (
        factservicerequest_df
          .join(crm_crmd_link_df,expr("SR.serviceRequestGUID = L.hiGUID"),"Inner")  
          .join(crm_scapptseg_df,expr("S.ApplicationGUID = L.setGUID"),"Inner")
          .join(dimtransactiondate_df,expr("T._businessKey = concat(S.applicationGUID, '|', S.apptType)"),"Inner")
          .filter(expr("L.setObjectType = '30'"))
          .filter(expr("SR.createdDateTime >= T.createdDateTime"))
          .selectExpr("SR.serviceRequestSK as primaryFK","T.transactionDateSK as secondaryFK","'Service Request - Date' as relationshipType")
    )
    
    servReq_attachment_df = (
        crm_skwg_brel_df
          .join(factservicerequest_df,expr("B.instanceIDA = SR.serviceRequestGUID"),"Inner")  
          .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
          .join(dimattachmentinfo_df,expr("A.documentID = O.documentID"),"Inner")
          .filter(expr("SR.changeDateTime >=O.creationDatetime"))
          .selectExpr("SR.serviceRequestSK as primaryFK","A.attachmentInfoSK as secondaryFK","'Service Request - Attachment' as relationshipType")
    )
        
    intern_attachment_df = (
        crm_skwg_brel_df
          .join(factinteraction_df,expr("B.instanceIDA = IR.interactionGUID"),"Inner")  
          .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
          .join(dimattachmentinfo_df,expr("A.documentID = O.documentID"),"Inner")
          .filter(expr("IR.createdDate >=O.creationDatetime"))
          .selectExpr("IR.InteractionSK as primaryFK","A.attachmentInfoSK as secondaryFK","'Interaction - Attachment' as relationshipType")
    )
    
    
    df = (
        wn_summary_df
        .union(wn_resolution_df)
        .union(interaction_wn_df)
        .union(email_df)
        .union(interaction_email_df)
        .union(intern_servReq_df)
        .union(servReq_servReq_df)
        .union(servReq_intern_df)
        .union(servReq_date_df)
        .union(servReq_attachment_df)
        .union(intern_attachment_df)
        
    )    
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"primaryFK||'|'||secondaryFK||'|'||relationshipType {BK}"
        ,"primaryFK"
        ,"secondaryFK"
        ,"relationshipType"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select relationshipType, count(*)
# MAGIC from curated_v2.bridgeservicerequest
# MAGIC group by relationshipType
# MAGIC order by 1
