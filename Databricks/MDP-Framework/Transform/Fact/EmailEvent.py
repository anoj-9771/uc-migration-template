# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_crmd_erms_step")
    crmd_erms_eventt_df = GetTable(f"{SOURCE}.crm_crmd_erms_eventt").select("emailEventID", "emailEventDescription")
    businessPartner_firstAgent_df = GetTable(f"{TARGET}.dimbusinessPartner").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_secondAgent_df = GetTable(f"{TARGET}.dimbusinessPartner").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_firstOrgUnit_df = GetTable(f"{TARGET}.dimbusinessPartner").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_secondOrgUnit_df = GetTable(f"{TARGET}.dimbusinessPartner").select("businessPartnerSK", "businessPartnerNumber")
    emailHeader_df = GetTable(f"{TARGET}.dimEmailHeader").select("emailHeaderSK","emailID").filter("_recordCurrent == 1")
    
    # ------------- JOINS ------------------ #
    df = df.join(crmd_erms_eventt_df, crmd_erms_eventt_df.emailEventID == df.eventID, "left").select(df["*"],crmd_erms_eventt_df["emailEventDescription"])
    df = df.join(businessPartner_firstAgent_df, df.firstAgent == businessPartner_firstAgent_df.businessPartnerNumber, "left").select(df["*"],businessPartner_firstAgent_df["businessPartnerSK"].alias("firstAgentSK"))
    df = df.join(businessPartner_secondAgent_df, df.secondAgent == businessPartner_secondAgent_df.businessPartnerNumber, "left").select(df["*"],businessPartner_secondAgent_df["businessPartnerSK"].alias("secondAgentSK"))
    df = df.join(businessPartner_firstOrgUnit_df, df.firstOrganisationUnit == businessPartner_firstOrgUnit_df.businessPartnerNumber, "left").select(df["*"],businessPartner_firstOrgUnit_df["businessPartnerSK"].alias("firstServiceTeamSK"))
    df = df.join(businessPartner_secondOrgUnit_df, df.secondOrganisationUnit == businessPartner_secondOrgUnit_df.businessPartnerNumber, "left").select(df["*"],businessPartner_secondOrgUnit_df["businessPartnerSK"].alias("secondServiceTeamSK"))
    df = df.join(emailHeader_df,"emailID","left")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"emailID {BK}"
        ,"right(emailID, 17) emailID"
        ,"eventID eventID"
        ,"emailEventDescription eventDescription"
        ,"eventTimestamp eventTimestamp"
        ,"stepNumber eventStepNumber"
        ,"case when stepDirection = 1 then 'Inbound' when stepDirection = 2 then 'Outbound' end emailEventDirection"
        ,"firstAgentSK firstAgentFK"
        ,"secondAgentSK secondAgentFK"
        ,"firstServiceTeamSK firstServiceTeamFK"
        ,"secondServiceTeamSK secondServiceTeamFK"
        ,"emailHeaderSK emailHeaderFK"
        ,"duration emailStepDuration"
        ,"totalDurationStep emailTotalStepDuration"
        ,"connectionStepTransferNumber emailTransferStepNumber"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


