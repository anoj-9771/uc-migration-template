# Databricks notebook source
def SaveChecklistTemplates(dfChecklists, dfWorkUnitLocales):

  #Alias columns
  dfCC=GeneralAliasDataFrameColumns(dfChecklists, "CC_")
  dfWUL=GeneralAliasDataFrameColumns(dfWorkUnitLocales, "WUL_")

  dfChecklistTemplates = dfCC.join(dfWUL, dfCC.CC_CheckListWorkUnitLocale == dfWUL.WUL_WorkUnitLocaleId)

    #Output Columns
  dfChecklistTemplates = dfChecklistTemplates.selectExpr(
        "CC_CheckListId as CheckListId"
      , "CC_Version as Version"
      , "CC_Sponsor as Sponsor"
      , "CC_CheckListAcronym as CheckListAcronym"
      , "CC_CheckListName as CheckListName"
      , "CC_CheckListSponsors as CheckListSponsors"
      , "WUL_WULPublicName as WULPublicName"
      , "CC_ImplementationDate as ImplementationDate"
      , "CC_LastUpdated as CC_LastUpdated"
      , "CC_ReviewDate as ReviewDate" 
    ).filter((col("CheckListAcronym")=="OFFERING-NEW") | (col("CheckListAcronym")=="OFFERING-AMND") |(col("CheckListAcronym")=="LRN-ConEx") | (col("CheckListAcronym")=="LRN-AppTrain") | (col("CheckListAcronym")=="LRN-CT") | (col("CheckListAcronym")=="LRN-CT/RPL") | (col("CheckListAcronym")=="LRN-RPL") | (col("CheckListAcronym")=="LRN-CT-RPL"))
  
  eCheckListSaveDataFrameToCurated(dfChecklistTemplates, 'ChecklistTemplates')

  #display(df)
  return dfChecklistTemplates

#display(getChecklistTemplates())
