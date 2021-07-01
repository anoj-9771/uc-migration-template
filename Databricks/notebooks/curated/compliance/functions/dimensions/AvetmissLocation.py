# Databricks notebook source
###########################################################################################################################
# Function: GetAvetmissLocation
#  GETS LOCATION DIMENSION 
# Parameters: 
#  locationDf = OneEBS_EBS_0165_locations
#  orgUnitsDf = OneEBS_EBS_0165_organisation_units
#  orgUnitLinksDf = OneEBS_EBS_0165_org_unit_links
#  addressesDf = OneEBS_EBS_0165_addresses
#  instituteDf = reference.institute
# Returns:
#  Dataframe of transformed Location
#############################################################################################################################
def GetAvetmissLocation(locationDf, orgUnitsDf, orgUnitLinksDf, addressesDf, instituteDf, startDate, endDate):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(locationDf, "l_")
  oucDf = GeneralAliasDataFrameColumns(orgUnitsDf, "ouc_")
  oulDf = GeneralAliasDataFrameColumns(orgUnitLinksDf, "oul_")
  oupDf = GeneralAliasDataFrameColumns(orgUnitsDf, "oup_")
  oulrDf = GeneralAliasDataFrameColumns(orgUnitLinksDf, "oulr_")
  ouprDf = GeneralAliasDataFrameColumns(orgUnitsDf, "oupr_")
  arDf = GeneralAliasDataFrameColumns(addressesDf, "ar_")
  apDf = GeneralAliasDataFrameColumns(addressesDf, "ap_")
  aiDf = GeneralAliasDataFrameColumns(addressesDf, "ai_")
  aipDf = GeneralAliasDataFrameColumns(addressesDf, "aip_")
  riDf = GeneralAliasDataFrameColumns(instituteDf, "ri_")

  #JOIN TABLES
  df = df.join(oucDf, expr("ouc_ORGANISATION_CODE = l_ORGANISATION_CODE AND ouc_ORGANISATION_TYPE = 'CO'"), how="left") #College/Campus
  df = df.join(oulDf, expr("oul_SECONDARY_ORGANISATION = ouc_ORGANISATION_CODE"), how="left")
  df = df.join(oupDf, expr("oup_ORGANISATION_CODE = oul_PRIMARY_ORGANISATION"), how="left") #Institute
  df = df.join(oulrDf, expr("oulr_SECONDARY_ORGANISATION = oup_ORGANISATION_CODE"), how="left") 
  df = df.join(ouprDf, expr("oupr_ORGANISATION_CODE = oulr_PRIMARY_ORGANISATION AND oupr_ORGANISATION_TYPE = 'REG'"), how="left") #Region
  df = df.join(arDf, expr("ar_OWNER_REF = l_ORGANISATION_CODE AND ar_ADDRESS_TYPE = 'RES'"), "left") #Address RES
  df = df.join(apDf, expr("ap_OWNER_REF = l_ORGANISATION_CODE AND ap_ADDRESS_TYPE = 'POS'"), "left") #Address POS
  df = df.join(aiDf, expr("ai_OWNER_REF = oup_ORGANISATION_CODE AND ai_ADDRESS_TYPE = 'RES'"), "left") #Address RES
  df = df.join(aipDf, expr("aip_OWNER_REF = oup_ORGANISATION_CODE AND aip_ADDRESS_TYPE = 'POS'"), "left") #Address POS
  df = df.join(riDf, expr("RIGHT(oup_organisation_code, 3) = ri_InstituteID"), how="left") #Reference Institute
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	"CAST(l_site_code AS VARCHAR(50)) as LocationID /*01M-EDWDM-438*/"
	,"l_location_CODE as LocationCode /*02M-EDWDM-449*/"
    ,"l_organisation_code as LocationOrganisationCode"
	,"l_FES_SHORT_DESCRIPTION as LocationName /*03M-EDWDM-450*/"
	,"l_FES_LONG_DESCRIPTION as LocationDescription /*04M-EDWDM-451*/"
	,"l_TOWN as LocationSuburb /*PRADA-1220*/"
	,"l_UK_POST_CODE_PT1 as LocationPostCode /*PRADA-1220*/"
    ,"l_address_line_1 AddressLine1"
	,"l_address_line_1 AddressLine2"
	,"l_town Suburb"
	,"l_region AusState"
	,"l_UK_POST_CODE_PT1 Postcode"
	,"l_FES_ACTIVE as Active"
	,"COALESCE(oup_ORGANISATION_CODE, 'N/A') as InstituteID/*07M-EDWDM-454*/"
    ,"COALESCE(ri_InstituteCode, 'N/A') InstituteCode /*08T-EDWDM-805*/"
	,"COALESCE(REPLACE(oup_fes_full_name, ' Institute',''), 'Inactive') as InstituteName /*09T-EDWDM-806*/"
    ,"COALESCE(oup_fes_full_name, 'Inactive') as InstituteDescription /*10M-EDWDM-455*/ "
    ,"COALESCE(ri_RTOCode, 'N/A') RTOCode /*11T-EDWDM-807*/"
	,"COALESCE(ai_address_line_1, aip_address_line_1) InstituteAddressLine1"
	,"CASE WHEN ai_address_line_1 IS NOT NULL THEN ai_address_line_2 ELSE aip_address_line_2 END InstituteAddressLine2"
	,"CASE WHEN ai_address_line_1 IS NOT NULL THEN ai_town ELSE aip_town END InstituteSuburb"
	,"CASE WHEN ai_address_line_1 IS NOT NULL THEN ai_region ELSE aip_region END InstituteAusState"
	,"CASE WHEN ai_address_line_1 IS NOT NULL THEN ai_uk_post_code_pt1 ELSE aip_uk_post_code_pt1 END InstitutePostcode"
	,"COALESCE(oupr_organisation_code, 'N/A') as RegionID /*12M-EDWDM-456*/"
	,"COALESCE(oupr_fes_full_name, 'Inactive') as RegionName /*13M-EDWDM-457*/"
    ,"ROW_NUMBER() OVER( PARTITION BY l_location_CODE ORDER BY ar_Start_date desc) AS AddressRank /*PRADA-1095*/"
  ) \
    .where("CAST(l_LOCATION_CODE AS INT) IS NULL AND l_SITE_CODE IS NOT NULL")
  
  df = df.withColumn("InstituteName", expr("TidyCase(LOWER(REPLACE(REPLACE(InstituteName, ' INSTITUTE', ''), ' ', '_')))")) \
    .withColumn("InstituteDescription", expr("TidyCase(LOWER(REPLACE(InstituteDescription, ' ', '_')))")) \
    .withColumn("LocationSuburb", expr("TidyCase(LOWER(REPLACE(LocationSuburb, ' ', '_')))")) \
    .where(expr("AddressRank == 1")).drop("AddressRank") \
    .orderBy("LocationCode")

  return df

# COMMAND ----------


