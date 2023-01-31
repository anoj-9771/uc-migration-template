# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_crmd_erms_header")
    crmd_erms_contnt_df = GetTable(f"{SOURCE}.crm_crmd_erms_contnt").select("EmailID", "subject")
    businessPartner_agent_df = GetTable(f"curated_v2.dimbusinessPartner").select("businessPartnerSK", "businessPartnerNumber")
    businessPartner_orgunit_df = GetTable(f"curated_v2.dimbusinessPartner").select("businessPartnerSK", "businessPartnerNumber")
    
    # ------------- JOINS ------------------ #
    df = df.join(crmd_erms_contnt_df, crmd_erms_contnt_df.EmailID == df.emailID, "left").select(df["*"],crmd_erms_contnt_df["subject"])
    df = df.join(businessPartner_agent_df, df.responsibleAgent == businessPartner_agent_df.businessPartnerNumber, "left").select(df["*"],businessPartner_agent_df["businessPartnerSK"].alias("responsibleAgentSK"))
    df = df.join(businessPartner_orgunit_df, concat(lit("OU"), col("organisationUnit").substr(-8,8)) == businessPartner_orgunit_df.businessPartnerNumber, "left").select(df["*"],businessPartner_orgunit_df["businessPartnerSK"].alias("serviceTeamSK"))

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"emailID {BK}"
        ,"right(emailID, 17) emailID"
        ,"case when connectionDirection = 1 then ""externalEmailAddress"" when connectionDirection = 2 then ""internalEmailAddress"" end emailSender"
        ,"case when connectionDirection = 1 then ""internalEmailAddress"" when connectionDirection = 2 then ""externalEmailAddress"" end emailRecepient"
        ,"emailStatusCode emailStatusCode"
        ,"emailStatus emailStatusDescription"
        ,"case when connectionDirection = 1 then 'Inbound' when connectionDirection = 2 then 'Outbound' end emailDirection"
        ,"serviceTeamSK serviceTeamFK"
        ,"handlingTime emailHandlingTime"
        ,"responseTime emailResponseTime"
        ,"workItemID workItemId"
        ,"responsibleAgentSK responsibleAgentFK"
        ,"escalationType escalationType"
        ,"escalationDateTime escalationDateTime"
        ,"escalationTimeInHours escalationTimeInHours"
        ,"subject emailSubject"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#     CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
