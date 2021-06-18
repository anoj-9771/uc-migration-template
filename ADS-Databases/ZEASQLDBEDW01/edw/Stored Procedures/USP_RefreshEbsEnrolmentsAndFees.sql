


/*********************************************************************************************
-- ===========================================================================================
-- Description:	Populates the fact table [EBS].[EnrolmentsAndFees] from EDW ebs tables
Example:		EXEC UMD.[USP_RefreshEbsEnrolmentsAndFees 

SELECT count(*) from EBS.EnrolmentsAndFees;
SELECT * from EBS.EnrolmentsAndFees;

-- Change History
Date		Modified By			Remarks
31/05/2021	Marcelle Folkerts	Added additional Fields & PK

-- ===========================================================================================
*********************************************************************************************/

CREATE procedure [edw].[USP_RefreshEbsEnrolmentsAndFees] 
as
	begin

		----------------------------
		-- SET environment variables
		----------------------------
		set nocount on; -- the count is not returned
		set xact_abort on; -- if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.

		begin try

			----------------------------------------------------------------------------------------------------
			-- Get Current Fee Values (CFV)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#CFV') is not null DROP TABLE [#CFV]
			SELECT COALESCE(PEOPLE_UNIT_LINKS.PARENT_ID, FEES_LIST.RUL_CODE) as PU_ID,  
					SUM(ISNULL(FEES_LIST.AMOUNT,0)) CFee  
			INTO [#CFV]
			FROM OneEBS.FEES_LIST
				INNER JOIN OneEBS.FEE_TYPES 
					ON FEE_TYPES.FEE_TYPE_CODE = FEES_LIST.FES_FEE_TYPE AND FEE_TYPES.FEE_CATEGORY_CODE IN ('COMM','TAFEFEE','TVET','TVH')
				LEFT JOIN OneEBS.PEOPLE_UNIT_LINKS 
					ON PEOPLE_UNIT_LINKS.CHILD_ID = FEES_LIST.RUL_CODE
				INNER JOIN OneEBS.FEE_VALUES 
					ON FEE_VALUES.FEE_VALUE_NUMBER = FEES_LIST.FEE_VALUE_NUMBER

				WHERE FEES_LIST.FES_RECORD_TYPE = 'F'
				AND FEES_LIST.[STATUS] = 'F' 

				GROUP BY COALESCE(PEOPLE_UNIT_LINKS.PARENT_ID, FEES_LIST.RUL_CODE) 	;

			----------------------------------------------------------------------------------------------------
			-- Get Fee Waiver Values (FWV)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#FWV') is not null DROP TABLE [#FWV]
			SELECT DISTINCT COALESCE(PEOPLE_UNIT_LINKS.PARENT_ID, FEES_LIST.RUL_CODE) PU_ID,
					 STRING_AGG(WAIVER_VALUES.WAIVER_CODE, ', ') WITHIN GROUP (ORDER BY WAIVER_VALUES.WAIVER_CODE) as WC
			INTO [#FWV]
			FROM OneEBS.FEES_LIST
				INNER JOIN OneEBS.FEE_VALUES 
					ON FEE_VALUES.FEE_VALUE_NUMBER = FEES_LIST.FEE_VALUE_NUMBER
				INNER JOIN OneEBS.FEE_TYPES 
					ON FEE_TYPES.FEE_TYPE_CODE = FEES_LIST.FES_FEE_TYPE AND FEE_TYPES.FEE_CATEGORY_CODE IN ('COMM','TAFEFEE','TVET','TVH')
				INNER JOIN OneEBS.FEES_LIST_WAIVERS 
					ON FEES_LIST_WAIVERS.FEE_RECORD_CODE = FEES_LIST.FEE_RECORD_CODE
				LEFT JOIN OneEBS.PEOPLE_UNIT_LINKS 
					ON PEOPLE_UNIT_LINKS.CHILD_ID = FEES_LIST.RUL_CODE
				INNER JOIN OneEBS.WAIVER_VALUES
					ON WAIVER_VALUES.WAIVER_VALUE_NUMBER = FEES_LIST_WAIVERS.WAIVER_VALUE_NUMBER AND WAIVER_VALUES.WAIVER_CODE IS NOT NULL
			WHERE FEES_LIST.FES_RECORD_TYPE = 'F'
			AND FEES_LIST.[STATUS] = 'F' 
			AND COALESCE(PEOPLE_UNIT_LINKS.PARENT_ID, FEES_LIST.RUL_CODE) is not null

			GROUP BY COALESCE(PEOPLE_UNIT_LINKS.PARENT_ID, FEES_LIST.RUL_CODE);

			----------------------------------------------------------------------------------------------------
			-- Get Enrolment Waivers (EW)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#EW') is not null DROP TABLE [#EW]
			SELECT distinct ATTACHED_WAIVER_VALUES.RUL_CODE,
				 STRING_AGG(WAIVER_VALUES.WAIVER_CODE, ', ')
				WITHIN GROUP (ORDER BY WAIVER_VALUES.WAIVER_CODE) as WC
			INTO [#EW]
			FROM OneEBS.ATTACHED_WAIVER_VALUES
				INNER JOIN OneEBS.WAIVER_VALUES 
					ON WAIVER_VALUES.WAIVER_VALUE_NUMBER = ATTACHED_WAIVER_VALUES.WAIVER_VALUE_NUMBER
				WHERE ATTACHED_WAIVER_VALUES.RUL_CODE is not null
				GROUP BY ATTACHED_WAIVER_VALUES.RUL_CODE;

			----------------------------------------------------------------------------------------------------
			-- Get Location (LOC)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#LOC') is not null DROP TABLE [#LOC]
			SELECT  l.location_CODE as LocationCode
					, l.FES_SHORT_DESCRIPTION as LocationName
					, l.FES_LONG_DESCRIPTION as LocationDescription
					, i.InstituteCode AS InstituteCode 	
					, REPLACE(oup.fes_full_name, ' Institute','') as InstituteName
					, oupr.fes_full_name as Region 
			INTO [#LOC]
			FROM edw.OneEBS_EBS_0165_locations l 
				LEFT JOIN edw.OneEBS_EBS_0165_organisation_units ouc on ouc.organisation_code = l.organisation_code and ouc.organisation_type = 'CO' and ouc.[_RecordCurrent] = 1 /* College/Campus */ 
				LEFT JOIN edw.OneEBS_EBS_0165_org_unit_links oul on oul.secondary_organisation = ouc.organisation_Code and oul.[_RecordCurrent] = 1
				LEFT JOIN edw.OneEBS_EBS_0165_organisation_units oup on oup.organisation_code = oul.primary_organisation and oup.organisation_type = 'IN' and oup.[_RecordCurrent] = 1/* Institute */ 
				LEFT JOIN edw.OneEBS_EBS_0165_org_unit_links oulr on oulr.secondary_organisation = oup.organisation_Code and oulr.[_RecordCurrent] = 1
				LEFT JOIN edw.OneEBS_EBS_0165_organisation_units oupr on oupr.organisation_code = oulr.primary_organisation and oupr.organisation_type = 'REG' and oupr.[_RecordCurrent] = 1 /* Region */
				LEFT JOIN Reference.Institute i on right(oup.organisation_code,3) = i.InstituteID
			where isnumeric(l.location_code)=0 AND L.SITE_CODE IS NOT NULL
					and L.FES_ACTIVE = 'Y' and L.[_RecordCurrent] = 1;

			----------------------------------------------------------------------------------------------------
			-- Get Residential Address Values (RAV)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#RAV') is not null DROP TABLE [#RAV]
			SELECT PERSON_CODE,
				MAX(ADDRESS_CODE) as Row_ID
			INTO [#RAV]
			FROM [OneEBS].[Addresses]
			WHERE (END_DATE IS NULL OR (END_DATE IS NOT NULL AND END_DATE > getdate())) 
				AND PERSON_CODE IS NOT NULL 
				AND (START_DATE IS NULL OR (START_DATE IS NOT NULL AND START_DATE < getdate())) 
				AND OWNER_TYPE='P'
				AND ADDRESS_TYPE='RES'
				AND UPPER(ADDRESS_LINE_1) NOT LIKE '%DO NOT USE%'
			GROUP BY PERSON_CODE;
			
			----------------------------------------------------------------------------------------------------
			-- Get Date Applied Values (APV)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#APV') is not null DROP TABLE [#APV]
			SELECT PR.PEOPLE_UNITS_ID,
					MIN(PR.CREATED_DATE) DtApplied
			INTO [#APV]
			FROM [OneEBS].[Progress_Records] PR
			INNER JOIN [OneEBS].[Progress_Codes] PC
				ON PC.TYPE_NAME = PR.RPC_TYPE_NAME
			WHERE PC.STUDENT_STATUS='A'
			GROUP BY PR.PEOPLE_UNITS_ID;

			----------------------------------------------------------------------------------------------------
			-- Get Date Enrolled Values (ENV)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#ENV') is not null DROP TABLE [#ENV]
			SELECT PR.PEOPLE_UNITS_ID,
					MIN(PR.CREATED_DATE) DtEnrolled
			INTO [#ENV]
			FROM [OneEBS].[Progress_Records] PR
			INNER JOIN [OneEBS].[Progress_Codes] PC
				ON PC.TYPE_NAME = PR.RPC_TYPE_NAME
			WHERE PC.STUDENT_STATUS = 'R'
			GROUP BY PR.PEOPLE_UNITS_ID;


			----------------------------------------------------------------------------------------------------
			-- Get Main Data (EnrolmentFeeData)
			----------------------------------------------------------------------------------------------------
			
			IF object_id('tempdb..#EnrolmentFeeData') is not null DROP TABLE [#EnrolmentFeeData]
			SELECT L.Region,
					L.InstituteName as SubRegion,  --RTO,
					UIO.SLOC_LOCATION_CODE as [Location], -- CampusCode,
					PE.REGISTRATION_NO as LearnerNo,
					PE.PERSON_CODE as PersonCode,
					PE.SURNAME as Surname,
					PE.FORENAME as FirstName,
					PEOPLE_USI.USI,
					PEOPLE_USI.DATE_USI_VERIFIED,
					PE.DATE_OF_BIRTH as DateOfBirth,
					DATEDIFF(yy, PE.DATE_OF_BIRTH, COALESCE(PUS.START_DATE,UIO.FES_START_DATE)) 
						- CASE WHEN (MONTH(PE.DATE_OF_BIRTH) >= MONTH(COALESCE(PUS.START_DATE,UIO.FES_START_DATE)))  
								AND DAY(PE.DATE_OF_BIRTH) > DAY(COALESCE(PUS.START_DATE,UIO.FES_START_DATE)) THEN 1 ELSE 0 END as AgeAtEnrolment, --Age at Time of Enrolment in Years
					LOC.FES_LONG_DESCRIPTION as CampusName,
					FAC.FES_FULL_NAME as FacultyName,
					UIO.FES_UINS_INSTANCE_CODE as CourseCode,
					UIO.CALOCC_OCCURRENCE_CODE as CalOccCode,
					SUBSTRING(COALESCE(UI.PROSP_USER_8,UI.FES_LONG_DESCRIPTION),1,255) as CourseName,
					COALESCE(PUS.START_DATE,UIO.FES_START_DATE) as EnrolmentStartDate,
					COALESCE(PUS.END_DATE,UIO.FES_END_DATE) as EnrolmentEndDate,
					PU.PROGRESS_CODE as ProgressCode,
					PU.PROGRESS_DATE as ProgressDate,
					PROGRESS_CODES.FES_LONG_DESCRIPTION AS Progress,
					PU.DESTINATION as ReasonCode,
					prgres.FES_LONG_DESCRIPTION AS ProgressReason,
					COALESCE(PU.NZ_FUNDING,UIO.NZ_FUNDING) as FundingSource,
					Funding.FES_LONG_DESCRIPTION AS FundingDescription,
					PU.COMMITMENT_IDENTIFIER,
					pu.subsidy_status as CID_Status,
					pu.user_1 as A_T_Flag,
					APPT.FES_LONG_DESCRIPTION AS ApprenticeTrainee,
					TRA.FES_LONG_DESCRIPTION AS ApprenticeTraineeType,		
					PU.TRAINING_CONTRACT_IDENTIFIER as TCID,
					PU.UIO_ID,
					UIO.OFFERING_TYPE as OfferingType,
					OT.FES_LONG_DESCRIPTION AS OfferingTypeDesc,
					UIO.FES_USER_1 as OfferingCode,
					Teach.FES_FULL_NAME as TeachingSection,	
						
					--Fees
					PUF.STUDENT_FEE_AMOUNT as TSN_StudentFee,
					PUF.FEE_TYPE_INDICATOR as TSN_FeeTypeIndicator, 
					FTI.FES_LONG_DESCRIPTION AS TSN_FeeTypeIndicatorDesc,
					FWV.WC as FeeWaivers,	
					EW.WC as EnrolmentWaivers,	
					CFV.CFee AS EBS_CurrentFee,	  
					Case when CFV.CFee is null then 'N' else 'Y' end as ebsFeeExist,
					--End Fees
								
					PU.WHO_TO_PAY as WhoToPay,
					SUBSTRING(AD.UK_POST_CODE_PT1,1,5) as PostCode,
					AD.REGION as ResidentialState,
					AD.TOWN as ResidentialLocality,
					PE.HIGHEST_POST_SCHOOL_QUAL,
					PSQ.FES_LONG_DESCRIPTION AS PostSchoolQualification,
					PU.WELFARE_STATUS,
					CL.CRN as PrimaryCRN,
					CL.SECONDARY_CRN,
					QUAL.FES_LONG_DESCRIPTION AS QualificationLevel,
					UI.NATIONAL_COURSE_CODE,
					PE.RESIDENTIAL_STATUS,
					Residential.FES_LONG_DESCRIPTION AS ResidentialStatus,
					PE.CURREMPSTATUS ,
					Employment.FES_LONG_DESCRIPTION AS EmploymentStatus,
					PU.USER_5 as WorksInNSW,
					PU.IS_ESP_CLIENT as ESPclient,
					PU.ESP_ORGANISATION,
					ESP.FES_LONG_DESCRIPTION AS EmploymentServicesProvider,
					PU.IS_ESP_REFERRAL as ESPreferral,
					PU.ESP_REFERRAL_IDENTIFIER,
					indig.FES_LONG_DESCRIPTION AS IndigenousStatus,
					disab.FES_LONG_DESCRIPTION AS Disability,
					disas.FES_LONG_DESCRIPTION AS DisabilityAssessment,

					--Additional Fields added MF 31/05/2021
					PU.ID as PU_ID,
					PE.PERSONAL_EMAIL as PersonalEmail,
					PE.COLLEGE_EMAIL as TAFEemail,
					PE.COLLEGE_LOGIN as TAFEID,
					PE.MOBILE_PHONE_NUMBER as Mobile,
					GetDate() as DataLoadDate,
					UIO.FES_START_DATE as OfferingStart,					
					UIO.FES_END_DATE as OfferingEnd,
					APV.DtApplied,
					ENV.DtEnrolled,
					UIO.OWNING_ORGANISATION as FacultyCode,
					PROGRESS_CODES.STUDENT_STATUS as ProgressType,
					(SELECT MIN(PR.CREATED_DATE) 
						FROM [OneEBS].[Progress_Records] PR
						WHERE PR.RPC_PROGRESS_TYPE='A'
						AND PR.PEOPLE_UNITS_ID=PU.ID) DateFirstActive

			INTO [#EnrolmentFeeData]
			FROM OneEBS.PEOPLE PE
				INNER JOIN OneEBS.PEOPLE_UNITS PU
					ON PU.PERSON_CODE = PE.PERSON_CODE
					AND PU.UNIT_TYPE IN ('A','R')
				INNER JOIN OneEBS.UNIT_INSTANCE_OCCURRENCES UIO
					ON UIO.UIO_ID = PU.UIO_ID AND UIO.CALOCC_CALENDAR_TYPE_CODE = 'COURSE'
				INNER JOIN OneEBS.UNIT_INSTANCES UI
					ON UI.FES_UNIT_INSTANCE_CODE = UIO.FES_UINS_INSTANCE_CODE 
				LEFT JOIN OneEBS.PEOPLE_UNITS_SPECIAL PUS
					ON PUS.PEOPLE_UNITS_ID = PU.ID
					AND PU.SPECIAL_DETAILS = 'Y'
				LEFT JOIN OneEBS.PEOPLE_USI 
					ON PEOPLE_USI.PERSON_CODE = PE.PERSON_CODE
				LEFT JOIN OneEBS.LOCATIONS LOC
					ON LOC.LOCATION_CODE = UIO.SLOC_LOCATION_CODE
				LEFT JOIN OneEBS.ORGANISATION_UNITS FAC
					ON FAC.ORGANISATION_CODE = UIO.OFFERING_ORGANISATION
				LEFT JOIN OneEBS.ORGANISATION_UNITS Teach 
					ON Teach.ORGANISATION_CODE = UIO.OWNING_ORGANISATION 

				LEFT JOIN #CFV CFV
					ON CFV.PU_ID = PU.ID	
				LEFT JOIN #FWV	 FWV
					ON FWV.PU_ID = PU.ID
				LEFT JOIN #EW	 EW
					ON EW.RUL_CODE = PU.ID
				LEFT JOIN OneEBS.PEOPLE_UNITS_FINANCIAL PUF
					ON PUF.PEOPLE_UNITS_ID = PU.ID
				LEFT JOIN #APV APV
					ON APV.PEOPLE_UNITS_ID = PU.ID
				LEFT JOIN #ENV ENV
					ON ENV.PEOPLE_UNITS_ID = PU.ID

				LEFT JOIN #LOC L
					ON L.LocationCode = UIO.SLOC_LOCATION_CODE 
				LEFT JOIN	[OneEBS].[Verifiers] AS APPT
					ON	APPT.RV_DOMAIN = 'TRAINEE_CODES'
					AND	APPT.LOW_VALUE = pu.user_1
				LEFT JOIN  	[OneEBS].[Verifiers] AS TRA 
					ON	TRA.LOW_VALUE = PU.APPRENTICE_TRAINEE_TYPE
					AND	TRA.RV_DOMAIN = 'TRAINEE_TYPE'
				LEFT JOIN #RAV RAV
					ON RAV.PERSON_CODE=PE.PERSON_CODE
				LEFT JOIN [OneEBS].[Addresses] AD
					ON AD.ADDRESS_CODE = RAV.Row_ID
				LEFT JOIN [OneEBS].[People_Centrelink] CL
					ON CL.PERSON_CODE=PE.PERSON_CODE
				LEFT JOIN	[OneEBS].[Verifiers] OT
					ON OT.RV_DOMAIN = 'OFFERING_TYPE'
					AND OT.LOW_VALUE = UIO.OFFERING_TYPE	
				LEFT JOIN  	[OneEBS].[Verifiers] QUAL 
					ON QUAL.LOW_VALUE = UI.AWARD_CATEGORY_CODE
					AND QUAL.RV_DOMAIN = 'QUAL_AWARD_CATEGORY_CODE'
				INNER JOIN	[OneEBS].[Progress_Codes] PROGRESS_CODES
					ON PROGRESS_CODES.[TYPE_NAME] = PU.PROGRESS_CODE
					--AND PROGRESS_CODES.STUDENT_STATUS = 'R'
				LEFT JOIN  	[OneEBS].[Verifiers] AS prgres
					ON	prgres.LOW_VALUE = PU.DESTINATION
					AND	prgres.RV_DOMAIN = 'DESTINATION'
				LEFT JOIN	[OneEBS].VERIFIERS AS Residential 
					ON	Residential.LOW_VALUE = PE.RESIDENTIAL_STATUS
					AND	Residential.RV_DOMAIN = 'RESIDENTIAL_STATUS'
				LEFT JOIN   [OneEBS].VERIFIERS AS Employment
					ON	Employment.LOW_VALUE = PE.CURREMPSTATUS 
					AND	Employment.RV_DOMAIN = 'CURRENT_EMPLOYMENT_STATUS'
				LEFT JOIN   [OneEBS].VERIFIERS AS ESP
					ON	ESP.LOW_VALUE = PU.ESP_ORGANISATION 
					AND	ESP.RV_DOMAIN = 'EMPLOYMENT_SERVICE_PROVIDER'
				LEFT JOIN   [OneEBS].VERIFIERS AS FTI 
					ON	FTI.LOW_VALUE = PUF.FEE_TYPE_INDICATOR
					AND FTI.RV_DOMAIN = 'FEE_TYPE_INDICATOR'
				LEFT JOIN   [OneEBS].VERIFIERS AS indig
					ON	indig.LOW_VALUE = PE.INDIGENOUS_STATUS
					AND	indig.RV_DOMAIN = 'INDIGENOUS_IDENTIFIER'
				LEFT JOIN   [OneEBS].VERIFIERS AS disab
					ON disab.LOW_VALUE  = PE.LEARN_DIFF
					AND disab.RV_DOMAIN = 'DISABILITY_STATUS'  
				LEFT JOIN   [OneEBS].VERIFIERS AS disas
					ON disas.LOW_VALUE  = PE.DISABILITY_ASSESSMENT_TYPE
					AND disas.RV_DOMAIN = 'DISABILITY_ASSESSMENT_TYPE'
				LEFT JOIN   [OneEBS].VERIFIERS AS Funding
					ON Funding.RV_DOMAIN = 'FUNDING'
					AND Funding.LOW_VALUE = COALESCE(PU.NZ_FUNDING,UIO.NZ_FUNDING) 
				LEFT JOIN	[OneEBS].VERIFIERS AS PSQ
				ON			PSQ.RV_DOMAIN = 'QUAL_SINCE_17'
				AND			PSQ.LOW_VALUE = PE.POST_SCHOOL_QUALS

		WHERE  COALESCE(PUS.END_DATE,UIO.FES_END_DATE) >=  dateadd(MM,-72,getdate())  
				AND	UPPER(PE.SURNAME) NOT LIKE '%DO NOT USE%'
				AND UPPER(PE.FORENAME) NOT LIKE '%DO NOT USE%' ;



			----------------------------------------------------------------------------------------------------
			-- Begin Transaction.
			----------------------------------------------------------------------------------------------------
			
			begin transaction;

			----------------------------------------------------------------------------------------------------
			-- Populate EBS.EnrolmentsAndFees
			----------------------------------------------------------------------------------------------------

			TRUNCATE TABLE [edw].[EBS_EnrolmentsAndFees];   
			ALTER TABLE [edw].[EBS_EnrolmentsAndFees] DROP CONSTRAINT IF EXISTS PK_EnrolmentsAndFee ;  --MF 31/05/2021

			INSERT INTO [edw].[EBS_EnrolmentsAndFees] (
					Region,
					SubRegion,  
					[Location], 
					LearnerNo,
					PersonCode,
					Surname,
					FirstName,
					USI,
					DateUSIverified,
					DateOfBirth,
					AgeAtEnrolment, 
					CampusName,
					FacultyName,
					CourseCode,
					CalOccCode,
					CourseName,
					EnrolmentStartDate,
					EnrolmentEndDate,
					ProgressCode,
					ProgressDate,
					Progress,
					ReasonCode,
					ProgressReason,
					FundingSource,
					FundingDescription,
					CommitmentIdentifier,
					CID_Status,
					A_T_Flag,
					ApprenticeTrainee,
					ApprenticeTraineeType,		
					TCID,
					UIO_ID,
					OfferingType,
					OfferingTypeDesc,
					OfferingCode,
					TeachingSection,	
					TSN_StudentFee,
					TSN_FeeTypeIndicator, 
					TSN_FeeTypeIndicatorDesc,
					FeeWaivers,	
					EnrolmentWaivers,	
					EBS_CurrentFee,	  
					ebsFeeExist,
					WhoToPay,
					PostCode,
					ResidentialState,
					ResidentialLocality,
					HighestPostSchoolQual,
					PostSchoolQualification,
					WelfareStatus,
					PrimaryCRN,
					SecondaryCRN,
					QualificationLevel,
					NationalCourseCode,
					RESIDENTIAL_STATUS,
					ResidentialStatus,
					CurrEMPstatus,
					EmploymentStatus,
					WorksInNSW,
					ESPclient,
					ESPOrganistaion,
					EmploymentServicesProvider,
					ESPreferral,
					ESPReferralIDentifier,
					IndigenousStatus,
					Disability,
					DisabilityAssessment,
					PU_ID,
					PersonalEmail,
					TAFEemail,
					TAFEID,
					Mobile,
					DataLoadDate,
					OfferingStart,					
					OfferingEnd,
					DtApplied,
					DtEnrolled,
					FacultyCode,
					ProgressType,
					DateFirstActive 	)
			
			SELECT Region,
					SubRegion,  
					[Location], 
					LearnerNo,
					PersonCode,
					Surname,
					FirstName,
					USI,
					DATE_USI_VERIFIED as DateUSIverified,
					DateOfBirth,
					AgeAtEnrolment, 
					CampusName,
					FacultyName,
					CourseCode,
					CalOccCode,
					CourseName,
					EnrolmentStartDate,
					EnrolmentEndDate,
					ProgressCode,
					ProgressDate,
					Progress,
					ReasonCode,
					ProgressReason,
					FundingSource,
					FundingDescription,
					COMMITMENT_IDENTIFIER as CommitmentIdentifier,
					CID_Status,
					A_T_Flag,
					ApprenticeTrainee,
					ApprenticeTraineeType,		
					TCID,
					UIO_ID,
					OfferingType,
					OfferingTypeDesc,
					OfferingCode,
					TeachingSection,	
					TSN_StudentFee,
					TSN_FeeTypeIndicator, 
					TSN_FeeTypeIndicatorDesc,
					FeeWaivers,	
					EnrolmentWaivers,	
					EBS_CurrentFee,	  
					ebsFeeExist,
					WhoToPay,
					PostCode,
					ResidentialState,
					ResidentialLocality,
					HIGHEST_POST_SCHOOL_QUAL as HighestPostSchoolQual,
					PostSchoolQualification,
					WELFARE_STATUS as WelfareStatus,
					PrimaryCRN,
					SECONDARY_CRN as SecondaryCRN,
					QualificationLevel,
					NATIONAL_COURSE_CODE as NationalCourseCode,
					RESIDENTIAL_STATUS,
					ResidentialStatus,
					CURREMPSTATUS as CurrEMPstatus,
					EmploymentStatus,
					WorksInNSW,
					ESPclient,
					ESP_ORGANISATION as ESPOrganistaion,
					EmploymentServicesProvider,
					ESPreferral,
					ESP_REFERRAL_IDENTIFIER as ESPReferralIDentifier,
					IndigenousStatus,
					Disability,
					DisabilityAssessment,
					PU_ID,
					PersonalEmail,
					TAFEemail,
					TAFEID,
					Mobile,
					DataLoadDate,
					OfferingStart,					
					OfferingEnd,
					DtApplied,
					DtEnrolled,
					FacultyCode,
					ProgressType,
					DateFirstActive
			FROM #EnrolmentFeeData;

			ALTER TABLE [edw].[EBS_EnrolmentsAndFees] WITH CHECK ADD CONSTRAINT PK_EnrolmentsAndFee PRIMARY KEY CLUSTERED (UniqueID) ;  --MF 31/05/2021

			commit transaction;

			--------------------
			Cleanup:
			--------------------
			if object_id('tempdb..#CFV') is not null drop table [#CFV]
			if object_id('tempdb..#FWV') is not null drop table [#FWV]
			if object_id('tempdb..#EW') is not null drop table [#EW]
			if object_id('tempdb..#LOC') is not null drop table [#LOC]
			if object_id('tempdb..#RAV') is not null drop table [#RAV]
			if object_id('tempdb..#APV') is not null drop table [#APV]
			if object_id('tempdb..#ENV') is not null drop table [#ENV]
			if object_id('tempdb..#EnrolmentFeeData') is not null drop table [#EnrolmentFeeData]

	End Try
	Begin Catch
	if @@tranCount > 0
		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)

	End Catch;
 END;