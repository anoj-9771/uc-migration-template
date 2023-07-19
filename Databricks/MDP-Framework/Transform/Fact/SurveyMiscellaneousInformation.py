# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import *

def transpose_df(df, cols_to_transpose, duplicate_columns):
        
    stack_expr = f"stack({len(cols_to_transpose)}, " + ", ".join([f"'{col}', cast({col} as string)" for col in cols_to_transpose]) + ")"


    transposed_df = df.selectExpr(
        stack_expr + " as (attributeName, attributeValue)",
        *duplicate_columns
    ) 
    
    return transposed_df

# COMMAND ----------

# Columns to transpose per response table #
billpaid_column = ["browserName", "browserVersionNumber",  "country", "customerFirstName" ,"customerLastName", "distributionChannel", "durationSeconds", "emailMeta", "emailSource", "endDate", 
                   "externalDataReference", "finished", "ipAddress", "locationLatitude", "locationLongitude", "operatingSystemText", "paidAmount", "paidDate", "paymentCode", "paymentMethod", "postalAddress",
                   "postcode","progressPercent", "propertyNumber", "recipientEmail", "recipientFirstName", "recipientLastName", "recordId", "recordedDate",  "sc", "screenResolution", "startDate", "state",
                   "status", "suburb", "supplyFullAddress", "supplyRank", "surveyStatus", "telephone", "totalDurationSeconds", "userLanguage"]


businessXconn_column = ["assignedTo", "browserName", "browserVersionNumber", "distributionChannel" , "durationSeconds", "endDate" , "finished", "ipAddress", "locationLatitude", "locationLongitude",
                       "operatingSystemText", "progressPercent", "recordId", "recordedDate", "screenResolution", "service", "serviceRequestNumber", "sla", "startDate", "status", "supportGroup", "userLanguage"]


complaintsClosed_column = ["browserName", "browserVersionNumber", "businessAreaResponsible", "clickCount", "communicationChannel", "companyName", "complaintActiveDays", "complaintCategory", "customerFirstName",
                           "customerLastName", "distributionChannel", "durationSeconds", "emailMeta","endDate", "externalDataReference", "finished", "firstClickSeconds", "ipAddress", "issueResponsibility", "lastClickSeconds",
                           "locationLatitude", "locationLongitude", "operatingSystemText", "pageSubmitDatetime", "postcode", "progressPercent", "propertyAddress", "receivedSubCategory", "recipientEmail", "recipientFirstName",
                           "recipientLastName", "recordId", "recordedDate", "resolutionCategory", "resolutionSubCategory", "resolutionSubCategory2", "resolutionSubCategory3", "screenResolution", "serviceRequestDateClosed",
                           "serviceRequestDateReceived", "serviceRequestNumber", "source", "startDate", "status", "suburb", "telephone", "totalDurationSeconds", "userLanguage"]

contactCentreInteract_column = ["activityPartner", "ageGroup", "category", "channel", "directionDesc", "distributionChannel", "durationSeconds", "endDate", "externalDataReference", "finished", "gCode","interactionDate",
                         "interactionTypeDesc","ipAddress","list", "locationLatitude", "locationLongitude", "objectId", "opp" ,"progressPercent", "qSedStatusDesc", "recipientEmail", "recipientFirstName", "recipientLastName", "recordId","recordedDate", "reportedByLastName", "reportedByPartnerNumber", "reportedbyFirstName", "reportedbyPersonEmail","reportedbyTelephoneNumber","startDate","status", "subCategory","surveyStatus",
                         "totalDurationSeconds", "userLanguage"]

Customercareresponses_column = ["activityPartner", "browserName","browserVersionNumber","category","channelDescription","distributionChannel","durationSeconds","endDate","externalDataReference","finished","interactionDate",
                         "ipAddress","list","locationLatitude","locationLongitude","operatingSystemText","progressPercent","recipientEmail","recipientFirstName","recipientLastName","recordId","recordedDate","reportedByLastName",
                         "reportedbyFirstName","reportedbyMobileNumber","reportedbyPersonEmail","screenResolution","serviceRequestNumber","startDate","status","subCategory","totalDurationSeconds","userLanguage"]


devApplicationreceived_column = ["applicant", "applicationId", "assessmentResolution", "browserName", "browserVersionNumber", "clickCount", "dateAssessed", "dateCreated", "dateSubmitted", "developer", "developerSegment", 
                         "distributionChannel", "durationSeconds","endDate","finished","firstClickSeconds","ipAddress","lastClickSeconds","locationLatitude","locationLongitude","operatingSystemText","pageSubmitDatetime","progressPercent",
                         "qSedStatusDesc","recipientEmail","recipientFirstName","recordId","recordedDate","screenResolution","startDate","status","totalDurationSeconds","userLanguage"]


feedbacktabgolive_column = ["browserName", "browserVersionNumber", "country", "currentPageUrl", "deviceType", "distributionChannel", "durationSeconds", "endDate", "finished","ipAddress", 
                            "landingPageUrl","language", "locationLatitude","locationLongitude","marketingCloudVisitorId","operatingSystemText","pageReferrer","pageScroll","pageTitle","progressPercent",
                            "recordId","recordedDate","screenResolution","siteHistory","siteReferrer","startDate", "status","totalVisitedPageCount","uniqueVisitedPageCount","userLanguage","visitorType"]


p4sonlinefeedback_column = ["distributionChannel", "durationSeconds", "endDate", "finished","ipAddress","locationLatitude", "locationLongitude", "progressPercent", "recaptchaScore","recordId", "recordedDate", 
                     "startDate","status","userLanguage"]


s73surveyresponse_column = ["agent", "applicationType","businessSegment", "caseCompleted", "caseNumber", "customerType", "dateReceived", "developerAddressText", "developerName", "developerPhoneNumber", 
                            "developmentLocationText", "developmentType", "distributionChannel", "durationSeconds", "endDate", "finished", "ipAddress", "locationLatitude", "locationLongitude", "progressPercent"]


waterfixpost_column = ["afterHours", "appointmentDate", "aptType", "browserName", "browserVersionNumber", "clickCount", "contactNumber", "contractorCost", "customerValue", "distributionChannel", "durationSeconds", "endDate",
                "ffApptId", "finished", "firstClickSeconds", "ipAddress", "lastClickSeconds", "locationLatitude", "locationLongitude", "operatingSystemText", "pageSubmitDatetime", "postcode", "preferredPaymentOption", 
                "progressPercent", "propertyAddress", "recipientEmail", "recipientFirstName", "recipientLastName", "recordId", "recordedDate", "screenResolution", "startDate", "status", "suburb", "totalDurationSeconds",
                "userLanguage"]


websitegolive_column = ["browserName","browserVersionNumber","country", "currentPageUrl", "deviceType", "distributionChannel","durationSeconds", "endDate","finished", "ipAddress", "landingPageUrl", "language",
                        "locationLatitude", "locationLongitude", "marketingCloudVisitorId", "operatingSystemText", "pageReferrer", "pageScroll", "pageTitle", "progressPercent", "recordId", "recordedDate",
                        "screenResolution", "siteHistory", "siteReferrer", "startDate", "status", "totalVisitedPageCount", "uniqueVisitedPageCount","userLanguage", "visitorType"]


wscs73exp_column = ["distributionChannel", "durationSeconds","endDate", "finished","progressPercent","recordId","recordedDate", "sec73IssueDate","startDate", "status", "userLanguage", "waterServicesCoordinatorText"]

duplicate_columns = ["surveyId", "surveyName", "recordId"]


# COMMAND ----------

df_billpaid =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_billpaidsuccessfullyresponses')}")
df_businessXconn =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_businessconnectservicerequestcloseresponses')}")
df_complaintsClosed  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_complaintscomplaintclosedresponses')}")
df_contactCentreInteract  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_contactcentreinteractionmeasurementsurveyresponses')}")
df_Customercareresponses  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_customercareresponses')}")
df_devApplicationreceived  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_developerapplicationreceivedresponses')}")
df_feedbacktabgolive  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_feedbacktabgoliveresponses')}")
df_p4sonlinefeedback  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_p4sonlinefeedbackresponses')}")
df_s73surveyresponse   =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_s73surveyresponses')}")
df_waterfixpost =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_waterfixpostinteractionfeedbackresponses')}")
df_websitegolive =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_websitegoliveresponses')}")
df_wscs73exp =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_wscs73experiencesurveyresponses')}")

billpaid_df = transpose_df(df_billpaid, billpaid_column, duplicate_columns)
businessXconn_df = transpose_df(df_businessXconn, businessXconn_column, duplicate_columns)
complaintsClosed_df = transpose_df(df_complaintsClosed, complaintsClosed_column, duplicate_columns)
contactCentreInteract_df = transpose_df(df_contactCentreInteract, contactCentreInteract_column, duplicate_columns)
Customercareresponses_df = transpose_df(df_Customercareresponses, Customercareresponses_column, duplicate_columns)
devApplicationreceived_df = transpose_df(df_devApplicationreceived, devApplicationreceived_column, duplicate_columns)
feedbacktabgolive_df = transpose_df(df_feedbacktabgolive, feedbacktabgolive_column, duplicate_columns)
p4sonlinefeedback_df = transpose_df(df_p4sonlinefeedback, p4sonlinefeedback_column, duplicate_columns)
s73surveyresponse_df = transpose_df(df_s73surveyresponse, s73surveyresponse_column, duplicate_columns)
waterfixpost_df = transpose_df(df_waterfixpost, waterfixpost_column, duplicate_columns)
websitegolive_df = transpose_df(df_websitegolive, websitegolive_column, duplicate_columns)
wscs73exp_df = transpose_df(df_wscs73exp, wscs73exp_column, duplicate_columns)

# COMMAND ----------

final_df = billpaid_df.union(businessXconn_df) \
    .union(complaintsClosed_df) \
    .union(contactCentreInteract_df) \
    .union(Customercareresponses_df) \
    .union(devApplicationreceived_df) \
    .union(feedbacktabgolive_df) \
    .union(p4sonlinefeedback_df) \
    .union(s73surveyresponse_df) \
    .union(waterfixpost_df) \
    .union(websitegolive_df) \
    .union(wscs73exp_df) 


resp_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimSurveyResponseInformation')}") \
          .select("surveyResponseInformationSK", "surveyResponseId")

finaldf = final_df.join(resp_df, resp_df["surveyResponseId"] == final_df["recordId"], "left").withColumn("sourceSystem", lit('Qualtrics').cast("string")) 

# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    #based on (sourceSystemCode, surveyId, surveyResponseId and surveyAttributeName)
    _.Transforms = [
        f"sourceSystem||'|'||surveyID||'|'||surveyResponseId||'|'||attributeName {BK}"
        ,"surveyID surveyId"
        ,"surveyResponseInformationSK surveyResponseInformationFK"
        ,"surveyName surveyName"       
        ,"attributeName surveyAttributeName" 
        ,"attributeValue surveyAttributeValue"
        ,"sourceSystem sourceSystemCode"
    ]
    
    df_final = df_final.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df_final.display()
    #CleanSelf()
    Save(df_final)
    #DisplaySelf()
Transform()
