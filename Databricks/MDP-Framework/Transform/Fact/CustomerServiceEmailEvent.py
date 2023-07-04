# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_erms_step')}") \
    .withColumn("email_BK",expr("right(emailID,17)")) \
    .withColumn("firstAgent_BK",expr("CASE WHEN firstAgent IS NULL THEN '-1' ELSE firstAgent END")) \
    .withColumn("secondAgent_BK",expr("CASE WHEN secondAgent IS NULL THEN '-1' ELSE firstAgent END")) \
    .withColumn("firstOrgUnit_BK",expr("CASE WHEN firstOrganisationUnit IS NULL THEN '-1' ELSE concat('OU',RIGHT(firstOrganisationUnit,8)) END")) \
    .withColumn("secondOrgUnit_BK",expr("CASE WHEN secondOrganisationUnit IS NULL THEN '-1' ELSE concat('OU',RIGHT(secondOrganisationUnit,8)) END"))        
    crmd_erms_eventt_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_erms_eventt')}").select("emailEventID", "emailEventDescription")
    businessPartner_firstAgent_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimbusinessPartner')}").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_secondAgent_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimbusinessPartner')}").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_firstOrgUnit_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimbusinessPartner')}").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_secondOrgUnit_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimbusinessPartner')}").select("businessPartnerSK", "businessPartnerNumber")
    emailHeader_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimcustomerserviceemailheader')}").select("customerServiceEmailHeaderSK","_businessKey").filter("_recordCurrent == 1")
    
    # ------------- JOINS ------------------ #
    df = df.join(crmd_erms_eventt_df, crmd_erms_eventt_df.emailEventID == df.eventID, "left").select(df["*"],crmd_erms_eventt_df["emailEventDescription"])
    df = df.join(businessPartner_firstAgent_df, df.firstAgent_BK == businessPartner_firstAgent_df.businessPartnerNumber, "left").select(df["*"],businessPartner_firstAgent_df["businessPartnerSK"].alias("firstAgentSK"))
    df = df.join(businessPartner_secondAgent_df, df.secondAgent_BK == businessPartner_secondAgent_df.businessPartnerNumber, "left").select(df["*"],businessPartner_secondAgent_df["businessPartnerSK"].alias("secondAgentSK"))
    df = df.join(businessPartner_firstOrgUnit_df, df.firstOrgUnit_BK == businessPartner_firstOrgUnit_df.businessPartnerNumber, "left").select(df["*"],businessPartner_firstOrgUnit_df["businessPartnerSK"].alias("firstServiceTeamSK"))
    df = df.join(businessPartner_secondOrgUnit_df, df.secondOrgUnit_BK == businessPartner_secondOrgUnit_df.businessPartnerNumber, "left").select(df["*"],businessPartner_secondOrgUnit_df["businessPartnerSK"].alias("secondServiceTeamSK"))
    df = df.join(emailHeader_df,df.email_BK == emailHeader_df._businessKey ,"left")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"emailID ||'|'||stepNumber {BK}"
        ,"right(emailID, 17) customerServiceEmailEventEmaiId"
        ,"eventID customerServiceEmailEventId"
        ,"emailEventDescription customerServiceEmailEventDescription"
        ,"eventTimestamp customerServiceEmailEventTimestamp"
        ,"stepNumber customerServiceEmailEventStepNumber"
        ,"case when stepDirection = 1 then 'Inbound' when stepDirection = 2 then 'Outbound' end customerServiceEmailEventDirectionIdentifier"
        ,"firstAgentSK firstAgentFK"
        ,"secondAgentSK secondAgentFK"
        ,"firstServiceTeamSK firstServiceTeamFK"
        ,"secondServiceTeamSK secondServiceTeamFK"
        ,"customerServiceEmailHeaderSK customerServiceEmailHeaderFK"
        ,"duration customerServiceEmailEventStepDurationHourQuantity"
        ,"totalDurationStep customerServiceEmailEventTotalStepDurationHourQuantity"
        ,"connectionStepTransferNumber customerServiceEmailEventTransferStepNumber"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#   display(df)
    #CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
