
CREATE view [compliance].[vwAvetmissUnitEnrolment]
as




with valid as (
select   ReportingYear,reportingdate, AvetmissClientId, AvetmissunitCode,AvetmissCourseCode, [UnitDeliveryLocationCode],ValidFlag, unitofferingenrolmentid--, row_number() over(partition by ReportingYear, AvetmissClientId, AvetmissUnitCode, AvetmissCourseCode, [UnitDeliveryLocationCode], ValidFlag  order by  CourseFundingSourceCode desc) rn
from compliance.AvetmissUnitEnrolment
where 1=1
and _recordcurrent = 1
and _recorddeleted = 0
and IncludeFlag = 1 
and validflag = 1
),

invalid as (
select   ReportingYear,reportingdate, AvetmissClientId, AvetmissunitCode,AvetmissCourseCode, [UnitDeliveryLocationCode], ValidFlag, unitofferingenrolmentid--, row_number() over(partition by ReportingYear, AvetmissClientId, AvetmissUnitCode, AvetmissCourseCode, [UnitDeliveryLocationCode], ValidFlag  order by  CourseFundingSourceCode desc) rn
from compliance.AvetmissUnitEnrolment
where 1 = 1
and _recordcurrent = 1
and _recorddeleted = 0
and IncludeFlag = 1 
and validflag = 0

),
Excludes as  (
				Select * from invalid where exists ( select 1 
													from valid 
													where invalid.ReportingYear = valid.ReportingYear 
													and invalid.reportingdate  = valid.reportingdate
													and invalid.AvetmissClientId  = valid.AvetmissClientId
													and invalid.AvetmissCourseCode  = valid.AvetmissCourseCode 
													and invalid.UnitDeliveryLocationCode  = valid.UnitDeliveryLocationCode)
				)



	select 
	
ue.ID_PC as ID_PC,

UE.ID_UUIO as ID_UUIO,
NULL as ID_CUIO,
ue.ID_CPU as ID_CPU,
ue.ID_UPU  as ID_UPU,
ue.[InstituteId] as INSTITUTE, 
[UnitDeliveryLocationCode] as LOCATION_CODE,
Right(ue.ReportingYear, 4) AS ENROLMENT_YEAR,
CONVERT(VARCHAR(10), ue.reportingDate, 112) AS REPORT_DATE, 
convert(varchar(8),cast(ue.ReportingDate as date),112)     AS 'EXTRACT_DATE', 
UnitEnrolmentStartDate as 'START_DATE',
DAY(UnitEnrolmentStartDate) AS 'Start Date Day', 
DATENAME(MONTH, UnitEnrolmentStartDate) AS 'Start Date Month',
YEAR(UnitEnrolmentStartDate) AS 'Start Date Year',
MONTH(UnitEnrolmentStartDate) AS SORT_SD,
UnitEnrolmentEndDate as 'END_DATE',
DAY(UnitEnrolmentEndDate) AS 'End Date Day', 
DATENAME(MONTH, UnitEnrolmentEndDate) AS 'End Date Month',
YEAR(UnitEnrolmentEndDate) AS 'End Date Year',
MONTH(UnitEnrolmentEndDate) AS SORT_ED,
UnitAwardDate AS DATE_AWARDED,
DAY(UnitAwardDate) AS 'Date Awarded Day', 
DATENAME(MONTH, UnitAwardDate) AS 'Date Awarded Month',
YEAR(UnitAwardDate) AS 'Date Awarded Year',
MONTH(UnitAwardDate) AS SORT_DA,
ue.UnitLevelStatus AS 'Award Status',
ue.UnitOfferingCode AS 'Calocc Code',  --
Coalesce(CommencingProgramIdentifier, 'Unit of competency or module enrolment only') AS 'Commencing Program Identifier', 
CASE
WHEN ue.ValidFlag = 1 THEN 'Valid'
ELSE 'Invalid'
END AS 'Count Valid Unit Enrolments', 


		CASE 
			WHEN [AvetmissDeliveryMode] IS NOT NULL THEN Concat([AvetmissDeliveryMode], ' ', AvetmissDeliveryModePre2018.[AvetmissDeliveryModeDescription] )
			ELSE NULL
		END




AS 'Delivery Mode - AVETMISS',



CASE
	WHEN [DeliveryMode] IS NOT NULL THEN compliance.ProperCase( [DeliveryMode].[DeliveryModeDescription])
	ELSE NULL
END AS 'Delivery Mode - TAFE', 
CASE
	WHEN FINALUNIT = 1 THEN 'Final Unit' 
	ELSE 'Not Final Unit'
END AS 'Final Unit', 

		CASE 
			WHEN af.AvetmissFundID IS NOT NULL THEN CONCAT(af.AvetmissFundID, ' ', af.AvetmissFundName)
			ELSE NULL
		END AS 'Funding - AVETMISS',
		CASE 
			WHEN fs.FundingSourceCode IS NOT NULL THEN CONCAT(fs.FundingSourceCode, ' ',fs.FundingSourceDescription)
			ELSE NULL 
		END AS 'Funding - TAFE', 


		CASE 
			WHEN fs.SpecificFundingCodeID IS NOT NULL THEN CONCAT( fs.SpecificFundingCodeID , ' ' ,[SpecificFundingName] ) 
			ELSE NULL 
		END AS 'Funding Specific', 

CASE 
	WHEN AcademicHistoryMissingFlag = 1 THEN 'No Academic History'
	ELSE 'Academic History'
END AS 'No Academic History', 


CASE 
	WHEN CONCAT(right(ue.ReportingYear, 2), ue.[InstituteId], UnitOfferingEnrolmentID) IS NOT NULL THEN 'Unit Enrolment'
	ELSE 'No Unit Enrolment'
END AS 'No Unit Enrolment', 

CASE	WHEN OUTCOMENATIONAL = '61' then '61 Superseded Subject'
		WHEN OUTCOMENATIONAL = '81' then '81 Non Assess - Satisfactorily Completed'
		WHEN OUTCOMENATIONAL = '82' then '82 Non Assess - Withdrawn or not Satisfactorily Completed'
	WHEN OUTCOMENATIONAL IS NOT NULL THEN CONCAT(OUTCOMENATIONAL, ' ', [AvetmissOutcomeDescription]) 
	ELSE NULL 
END AS 'Outcome - AVETMISS', 


Case when ue.Grade is not null 
Then 
     Case when  isnumeric(grade  ) = 1 then 'Hours Only'
	 when grade = 'NC' Then 'NC Not yet Competent'
	 when grade = 'T' Then 'T Tuition'
	 when grade = 'WN' Then 'WN Withdrawn no Penalty'
	 Else Concat(grade, ' ', [Grades].GradeDescription)  --Lauren will add this as a ref table
	 
End
End


AS 'Outcome - TAFE',   



UnitProgressCode AS 'Progress Code', 
UnitProgressStatus AS 'Progress Status', 
		
CASE 
	WHEN UnitProgressReason IS NOT NULL THEN pr.ProgressReasonDescription
	ELSE NULL 
END AS 'Progress Reason',
DAY(UnitProgressDate) AS 'Progress Date Day', 
DATENAME(MONTH, UnitProgressDate) AS 'Progress Date Month',
YEAR(UnitProgressDate) AS 'Progress Date Year',
MONTH(UnitProgressDate) AS SORT_PD,
		
CASE 
	WHEN [UnitRecognitionType] IS NOT NULL THEN CONCAT([UnitRecognitionType], ' ', RecognitionType.[RecognitionTypeDescrtiption] )
	ELSE NULL
END AS 'Recognition Type', 
		

ue.SponsorCode	AS 'Sponsor Org Code'	,
CASE 
	WHEN ue.StudyReasonID IS NOT NULL THEN CONCAT(cast(ue.StudyReasonID as int), ' ', ue.StudyReasonID, '. ', StudyReason.[StudyReasonDescription] )
	ELSE NULL
END AS 'Study Reason',

		
CASE 
	WHEN ue.ValidFlag = 1 THEN 'Finalised'
	ELSE 'Not Finalised'
END AS 'Valid Unit', 
	
CASE 
	WHEN VetInSchoolFlag = 'Y' THEN 'Yes - VET in School'
	ELSE 'No - VET in School'
END AS 'VET in School Flag', 

		case 
			when AvetmissLocation.LocationName = 'B2B / Partner' then 'Business to Business/Partnership' 
			when AvetmissLocation.LocationName = 'N Beaches' then 'Northern Beaches' 
			when AvetmissLocation.LocationName = 'Prim Ind Cent' then 'Primary Industries Centre' 
			when AvetmissLocation.LocationName = 'RI Learn Onli' then 'Riverina Institute Learn Online' 
			when AvetmissLocation.LocationName = 'Pt Macquarie' then 'Port Macquarie' 
			when AvetmissLocation.LocationName = 'Wollongong W' then 'Wollongong West' 
			when AvetmissLocation.LocationName = 'WSI Business' then 'Western Sydney Institute Business' 
			when AvetmissLocation.LocationName = 'Nat Env Ctre' then 'National Environmental Centre' 
			when AvetmissLocation.LocationName = 'Swsi Internat' then 'SWSI International' 
			when AvetmissLocation.LocationName = 'RIL' then 'Connected Learning Classroom' 
			when AvetmissLocation.LocationName = 'WSI Internati' then 'WSI International' 
			when AvetmissLocation.LocationName = 'Wit Access Un' then 'WIT Access Unit' 
			when AvetmissLocation.LocationName = 'Wetherill Pk' then 'Wetherill Park' 
			when AvetmissLocation.LocationName = 'OTEN' then 'OTEN - Distance Education' 
			when AvetmissLocation.LocationName = 'Online' then 'Online Campus' 
			when AvetmissLocation.LocationName = 'Nci Open Camp' then 'NCI Open Campus' 
			when AvetmissLocation.LocationName = 'Macquarie Fie' then 'Macquarie Fields' 
			when AvetmissLocation.LocationName = 'Lightning Rid' then 'Lightning Ridge' 
			when AvetmissLocation.LocationName = 'Lake Cargelli' then 'Lake Cargelligo' 
			when AvetmissLocation.LocationName = 'Flx E Trn Ill' then 'Flex E Training' 
			when AvetmissLocation.LocationName = 'Coffs Hbr Edc' then 'Coffs Harbour Education' 
			when AvetmissLocation.LocationName = 'Clarence CC' then 'Clarence Correctional Centre' 
			when AvetmissLocation.LocationName = 'YAMBA' then 'Yamba' 
			when AvetmissLocation.LocationName = 'Blue Mountain' then 'Blue Mountains' 
			when AvetmissLocation.LocationName = 'New Eng Campu' then 'New Eng Campus' 
			when AvetmissLocation.LocationName = 'Western Conne' then 'Western Connect' 
			else AvetmissLocation.LocationName 
		end as 'Delivery Location - AVETMISS',

ue.StudentId  AS 'Learner Number', 
UnitCode AS 'Unit Code', 
ue.AvetmissCourseCode AS 'Course Code', 
ue.AvetmissCourseCode AS 'Course Code - AVETMISS', 
		
		-- Measures
CASE WHEN IncludeFlag = 1 THEN 1 ELSE NULL END AS UNIT_COUNT, 
CASE WHEN outcomenational in ( '20','30','40','51','52','81','82')  THEN 1 ELSE 0 END AS VALID_UNIT_COUNT, -- and IncludeFlag = 1 and  ue.ValidFlag = 1
CASE WHEN AvetmissScheduledHours = 0 THEN NULL ELSE AvetmissScheduledHours END AS SCHED_HOURS_COUNT,
		-- ASH
--CASE WHEN 
CASE WHEN COALESCE(OutcomeNational,'') IN ('60','61') THEN 0 ELSE UnitScheduledHours END AS ASH_COUNT, 
CASE WHEN outcomenational  in ('70', '71')  THEN UnitScheduledHours ELSE NULL END AS ASH_CONT_COUNT, 
CASE WHEN FinalUnit = 0 THEN NULL ELSE AvetmissScheduledHours END AS ASH_FINAL_COUNT, 
CASE WHEN outcomenational  in ('51','52') THEN AvetmissScheduledHours ELSE NULL END AS ASH_RPL_COUNT, 
CASE WHEN outcomenational  in ('60','61','70','51','52')  THEN NULL	ELSE AvetmissScheduledHours END AS ASH_TRAINING_COUNT,
ue.Semester1Hours as SEM1HRS_COUNT,
ue.Semester2Hours as   SEM2HRS_COUNT,

		-- OUTCOMES 
CASE WHEN outcomenational  in ('70', '71')  THEN 1 ELSE NULL END AS CONT_UNITS_COUNT, 
CASE WHEN outcomenational  in ('60', '61')  THEN 1 ELSE NULL END AS CT_UNITS_COUNT, 
CASE WHEN outcomenational  in ('60', '51')  THEN 1 ELSE NULL END AS CT_RPL_UNITS_COUNT, 
CASE WHEN outcomenational  in ('30')  THEN 1 ELSE NULL END AS FAIL_UNIT_COUNT, 
CASE WHEN FINALUNIT = 1 THEN 1 ELSE NULL END AS FINAL_UNIT_COUNT, 
CASE WHEN outcomenational  in ('81', '82')  THEN 1 ELSE NULL END AS NON_ASS_UNIT_COUNT , 
CASE WHEN outcomenational  in ('82') THEN 1 ELSE NULL END AS NON_ASS_WDRAW_COUNT, 
CASE WHEN outcomenational  in ('81') THEN 1 ELSE NULL END AS NON_ASS_S_COMP_COUNT, 
CASE WHEN outcomenational  in ('20') THEN 1 ELSE NULL END AS PASS_UNIT_COUNT, 
CASE WHEN outcomenational  in ('51') THEN 1 ELSE NULL END AS RPL_GRANT_UNIT_COUNT, 
CASE WHEN outcomenational  in ('52') THEN 1 ELSE NULL END AS RPL_NOT_GRANT_UNIT_COUNT, 
CASE WHEN outcomenational  in ('51', '52') THEN 1 ELSE NULL END AS RPL_UNIT_COUNT, 
CASE WHEN outcomenational  in ('61') THEN 1 ELSE NULL END AS SUPERSEDED_UNIT_COUNT, 
CASE WHEN outcomenational  in ('40') THEN 1 ELSE NULL END AS WDRAW_UNIT_COUNT,
CASE WHEN DeliveryMode = 'ADVD' THEN 1 ELSE NULL END AS ADVA_COUNT,
CASE WHEN DeliveryMode = 'CLAS' THEN 1 ELSE NULL END AS CLAS_COUNT,
CASE WHEN DeliveryMode = 'SELF' THEN 1 ELSE NULL END AS SELF_COUNT,
CASE WHEN DeliveryMode = 'ELEC' THEN 1 ELSE NULL END AS ELEC_COUNT,
CASE WHEN DeliveryMode = 'BLND' THEN 1 ELSE NULL END AS BLND_COUNT,
CASE WHEN DeliveryMode = 'DIST' THEN 1 ELSE NULL END AS DIST_COUNT,
CASE WHEN DeliveryMode = 'ONJB' THEN 1 ELSE NULL END AS ONJB_COUNT,
CASE WHEN DeliveryMode = 'ONJD' THEN 1 ELSE NULL END AS ONJD_COUNT,
CASE WHEN DeliveryMode = 'SIMU' THEN 1 ELSE NULL END AS SIMU_COUNT,
CASE WHEN DeliveryMode = 'MISD' THEN 1 ELSE NULL END AS MISD_COUNT,
CASE WHEN DeliveryMode = 'OTHD' THEN 1 ELSE NULL END AS OTHD_COUNT,
Case when outcomenational  not in ('51','52','60','61')  then 1 else Null end as DELU_COUNT

From  compliance.AvetmissUnitEnrolment ue 
LEFT JOIN [REFERENCE].[STUDYREASON] ON UE.STUDYREASONID = [STUDYREASON].[STUDYREASONID] and [STUDYREASON]._RecordCurrent = 1 and [STUDYREASON]._RecordDeleted = 0
LEFT JOIN [reference].[RecognitionType]  ON UE.[UNITRECOGNITIONTYPE] = RECOGNITIONTYPE.[RecognitionTypeCode]  and [RecognitionType]._RecordCurrent = 1 and [RecognitionType]._RecordDeleted = 0
LEFT JOIN [reference].[AvetmissOutcome] ON AvetmissOutcome.AvetmissOutcomeID = UE.OUTCOMENATIONAL and [AvetmissOutcome]._RecordCurrent = 1 and [AvetmissOutcome]._RecordDeleted = 0
LEFT JOIN REFERENCE.DELIVERYMODE ON [DELIVERYMODE].[DELIVERYMODECODE] = UE.DELIVERYMODE and [DELIVERYMODE]._RecordCurrent = 1 and [DELIVERYMODE]._RecordDeleted = 0
Left join [reference].[DeliveryModeAvetmissDeliveryMode] on DeliveryModeAvetmissDeliveryMode.DeliveryModeCode =  UE.DELIVERYMODE and [DeliveryModeAvetmissDeliveryMode]._RecordCurrent = 1 and [DeliveryModeAvetmissDeliveryMode]._RecordDeleted = 0
LEFT JOIN REFERENCE.AvetmissDeliveryMode ON AvetmissDeliveryMode.[AvetmissDeliveryModeCode] = DeliveryModeAvetmissDeliveryMode.AvetmissDeliveryModeCode and AvetmissDeliveryMode._RecordCurrent = 1 and AvetmissDeliveryMode._RecordDeleted = 0
LEFT JOIN REFERENCE.AvetmissDeliveryModePre2018 ON AvetmissDeliveryModePre2018.[AvetmissDeliveryModeID] = UE.AVETMISSDELIVERYMODE and AvetmissDeliveryModePre2018._RecordCurrent = 1 and AvetmissDeliveryModePre2018._RecordDeleted = 0
left join Reference.FundingSource fs on fs.FundingSourceCode = UE.UnitFundingsourceCode and fs._RecordCurrent = 1 and fs._RecordDeleted = 0
left Join [reference].[AvetmissFund] af on af.AvetmissFundID = fs.FundingSourceNationalID and af._RecordCurrent = 1 and af._RecordDeleted = 0
left join reference.ProgressReason pr on pr.ProgressReasonCode = ue.UnitProgressReason and pr._RecordCurrent = 1 and pr._RecordDeleted = 0
left Join [reference].[SpecificFunding] sf on sf.[SpecificFundingCodeID] = fs.[SpecificFundingCodeID] and sf._RecordCurrent = 1 and sf._RecordDeleted = 0
left Join [reference].[Grades]  on [Grades].GradeCode = ue.Grade and [Grades]._RecordCurrent = 1 and [Grades]._RecordDeleted = 0
left join compliance.avetmissLocation on avetmissLocation.LocationCode =  UnitDeliveryLocationCode and avetmissLocation._RecordCurrent = 1 and avetmissLocation._RecordDeleted = 0


where LEFT(ue.ReportingYear, 2) = 'CY' 
and ue._RecordCurrent = 1
and ue._RecordDeleted = 0
and ue.IncludeFlag = 1 
and not exists (select 1 from excludes
				where excludes.REportingYear = ue.reportingyear
				  and excludes.reportingdate = ue.reportingdate
				  and excludes.AvetmissClientId = ue.AvetmissClientId
				  and excludes.AvetmissUnitCode = ue.AvetmissUnitCode
				  and excludes.AvetmisscourseCode = ue.AvetmisscourseCode 
				  and excludes.UnitDeliveryLocationCode = ue.UnitDeliveryLocationCode 
				  and excludes.validflag = ue.validflag

				  )