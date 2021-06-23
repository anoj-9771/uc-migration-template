

/*********************************************************************************************
-- ===========================================================================================
-- Description:	Populates the fact table [UMD].[FactUnenteredMarks] from EDW ebs tables

SELECT count(*) from UMD.FactUnenteredMarks;
SELECT * from UMD.FactUnenteredMarks;

SELECT count(*) from UMD.DimSkillsPoint;
SELECT * from UMD.DimSkillsPoint;

-- Change History
Date	Modified By		Ticket No		Remarks

-- ===========================================================================================
*********************************************************************************************/

CREATE procedure [UMD].[USP_RefreshUMDDataModel] 
as
	begin

		----------------------------
		-- SET environment variables
		----------------------------
		set nocount on; -- the count is not returned
		set xact_abort on; -- if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.

		begin try

		declare
				@vRefreshTimestamp datetime = getdate()

			----------------------------------------------------------------------------------------------------
			-- Get assessment counts
			----------------------------------------------------------------------------------------------------

			if object_id('tempdb..#asmt') is not null drop table [#asmt]
			select 
				[CAR].[PEOPLE_UNITS_ID] as [pu_id]
			  , count([CAR].[COURSE_ASSESSMENTS_ID]) as [assessment_count]
			  , sum(case
						when coalesce([CAR].[ADJUSTED_RESULT], [CAR].[RESULT], 'no mark') = 'no mark' then 0
						else 1
					end) as [result_count]
			  , '' as [Results]
			into [#Asmt]
			from [edw].[OneEBS_EBS_0165_COURSE_ASSESSMENTS_RESULTS] as [CAR]
				 inner join [edw].[OneEBS_EBS_0165_COURSE_ASSESSMENTS] as [CA] on [CAR].[COURSE_ASSESSMENTS_ID] = [CA].[ID]
			group by [CAR].[PEOPLE_UNITS_ID];

			-- ET: 00:01:10

			create clustered index idx_asmt_pu_id on #asmt (pu_id); -- ET: 00:00:13

			----------------------------------------------------------------------------------------------------
			-- Get main data
			----------------------------------------------------------------------------------------------------

			if object_id('tempdb..#marks1') is not null drop table [#marks1]
			select 
				[pu_unit].[PROGRESS_code] as [Unit_Prog_Code]
			  , [pu_course].[PROGRESS_CODE] as [Prod_Prog_Code]
			  , [prgres_course].[fes_long_description] as [Prod_Prog_Reason]
			  , [a].[per_person_code]
			  , [ui_unit].[unit_category] as [Unit_type]
			  , [uio_unit].[fes_uins_instance_code] as [unit_code]
			  , [uio_unit].[calocc_occurrence_code] as [unit_calocc]
			  , coalesce([pus_unit].[start_date], [uio_unit].[fes_start_date]) as [unit_start_dt]
			  , coalesce([pus_unit].[end_date], [uio_unit].[fes_end_date]) as [unit_end_dt]
			  , [gsg].[sdr_completion_code] as [final_outcome]
			  , [gsg].[grading_scheme_id]
			  , coalesce([a].[ADJUSTED_GRADE], [a].[grade]) as [Final_Grade]
			  , [a].[date_awarded] as [Awarded_Date]
			  , [cs].[status_code] as [UnitOutcome_Status]
			  , [a].[updated_date]
			  , [pu_unit].[id] as [pu_id]
			  , [pu_course].[commitment_identifier] as [cid]
			  , [uio_unit].[sloc_location_code]
			  , [pu_unit].[destination] as [UNIT_PROGRESS_REASON]
			  , [uio_unit].[uio_id] as [uio_id]
			  , [uio_unit].[FES_COURSE_CONTACT] as [Unit_Contact_Id]
			  , [pu_course].[id] as [course_id]
			  , [pu_course].[special_details] as [special_details_course]
			  , [pu_course].[uio_id] as [course_uioid]
			  , case
					when [pul].[CHILD_ID] is null then 'unlinked'
					else 'linked'
				end as [Link_Status]
			  , [pu_course].[subsidy_status] as [Claim_Status]
			into [#marks1]
			from [edw].[OneEBS_EBS_0165_ATTAINMENTS] as [a]
				 inner join [edw].[OneEBS_EBS_0165_PEOPLE_UNITS] as [pu_unit] on [a].[people_units_id] = [pu_unit].[id]
				 inner join [EDW].[OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES] as [uio_unit] on [pu_unit].[uio_id] = [uio_unit].[uio_id]
				 inner join [EDW].[OneEBS_EBS_0165_UNIT_INSTANCES] as [ui_unit] on [uio_unit].[fes_uins_instance_code] = [ui_unit].[fes_unit_instance_code]
				 left join [EDW].[OneEBS_EBS_0165_GRADING_SCHEME_GRADES] as [GSG] on coalesce([A].[ADJUSTED_GRADE], [A].[GRADE]) = [GSG].[GRADE] and [A].[GRADING_SCHEME_ID] = [GSG].[GRADING_SCHEME_ID]
				 left join [EDW].[OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL] as [pus_unit] on [pu_unit].[id] = [pus_unit].[PEOPLE_UNITS_ID] and [pu_unit].[special_details] = 'Y'
				 left join [EDW].[OneEBS_EBS_0165_CONFIGURABLE_STATUSES] as [cs] on [a].[configurable_status_id] = [cs].[id] and [status_type] = 'OUTCOME'
				 left join [EDW].[OneEBS_EBS_0165_PEOPLE_UNIT_LINKS] as [pul] on [pu_unit].[id] = [pul].[child_id]
				 left join [EDW].[OneEBS_EBS_0165_PEOPLE_UNITS] as [pu_course] on [pul].[parent_id] = [pu_course].[id]
				 left join [EDW].[OneEBS_EBS_0165_VERIFIERS] as [prgres_course] on [prgres_course].[low_value] = [pu_course].[destination] and [prgres_course].[rv_domain] = 'DESTINATION'
			where [pu_unit].[unit_type] = 'R'
				  and [ui_unit].[ui_level] = 'UNIT'
				  and [ui_unit].[unit_category] <> 'BOS'
				  and coalesce([pus_unit].[start_date], [uio_unit].[fes_start_date]) >= '2014-01-01'
				  and coalesce([a].[ADJUSTED_GRADE], [a].[grade]) is null
				  and coalesce([pus_unit].[end_date], [uio_unit].[fes_end_date]) < @vRefreshTimestamp
				  and [pu_unit].[progress_code] in ( '1.1 ACTIVE', '1.1 COND', '1.10VSLPEN', '1.11VSLAPP', '1.3 RPLREQ', '1.4 RPLNOG', '1.5 VFHPEN', '1.6 VFHAPP', '1.8FHAPP', '1.91TFCMAN', '1.92TFCERR', '3.1 WD', '4.1 COMPL' )
				  and [cs].[status_code] in ( 'PENDING', 'SUBMITTED' );

			-- ET: 00:00:47

			if object_id('tempdb..#claimable') is not null drop table [#claimable]
			
			select [uio_course].[fes_uins_instance_code], [uio_course].[calocc_occurrence_code], year([b].[start_date]) as start_date_year, count(1) as Cnt
			into #claimable
			from [EDW].[OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES] as [uio_course]
				join [EDW].[OneEBS_EBS_0165_UNIT_INSTANCES] as [ui_x] on [uio_course].[fes_uins_instance_code] = [ui_x].[FES_UNIT_INSTANCE_CODE]
				join [EDW].[OneEBS_EBS_0165_SKILLS_LIST_ITEMS] as [a] on [ui_x].[NATIONAL_COURSE_CODE] = [a].[qualification_code]
				join [EDW].[OneEBS_EBS_0165_SKILLS_LISTS] as [b] on [a].[SKILLS_LISTS_ID] = [b].[Id]
			where  ( [a].[IS_LOC_AVAIL_FND_SKILLS] = 'Y' or [a].[IS_LOC_AVAIL_APPRENTICESHIP] = 'Y' or [a].[IS_LOC_AVAIL_TRAINEESHIP] = 'Y' ) --or [a].[is_nat_avail_full_qual] = 'Y'
				and [offering_type] in ( 'SMART', 'SMARTNVH', 'SMARTVSL', 'TVET' )
			group by [uio_course].[fes_uins_instance_code], [uio_course].[calocc_occurrence_code], year([b].[start_date])
			
			
			if object_id('tempdb..#marks') is not null drop table [#marks]
			select distinct -- Required to remove 2 extra rows (needs further investigation).
				[fac].[fes_full_name] as [Faculty]
			  , [lo].[fes_long_description] as [College]
			  , [org].[fes_full_name] as [Teaching_Section]
			  , datepart([YYYY], [Marks1].[UNIT_END_DT]) as 'YEAR/PERIOD'
			  , [p].[forename] as [First_Name]
			  , [p].[surname] as [Last_Name]
			  , [Marks1].[cid]
			  , [Marks1].[pu_id]
			  , coalesce([pus_course].[start_date], [uio_course].[fes_start_date]) as [PROD_START_DT]
			  , coalesce([pus_course].[end_date], [uio_course].[fes_end_date]) as [PROD_END_DT]
			  , [uio_course].[fes_uins_instance_code] as [PROD_CODE]
			  , [ui_course].[fes_long_description] as [PROD_NAME]
			  , [ui_course].[fes_unit_instance_code] as [fes_unit_instance_code]
			  , [uio_course].[calocc_occurrence_code] as [PROD_CALOCC]
			  , case
					when [p].[REGISTRATION_NO] is null or [p].[registration_no] = '0' then 'Z'+convert(varchar(120), [p].[person_code])
					else convert(varchar(120), [p].[registration_no])
				end as [LEARNER_NO]
			  , [uio_course].[offering_type] as [PRODUCT_OFFER_TYPE]
			  , [Marks1].[UNIT_CODE]
			  , [Marks1].[UNIT_CALOCC]
			  , [Marks1].[UNIT_TYPE]
			  , [Marks1].[UNIT_PROG_CODE]
			  , [Marks1].[UNIT_START_DT]
			  , [Marks1].[UNIT_END_DT]
			  , [Marks1].[FINAL_OUTCOME]
			  , [Marks1].[FINAL_GRADE]
			  , [gs].[code] as [GRADE_CODE]
			  , [gs].[description] as [GRADING_CODE_DESC]
			  , [Marks1].[AWARDED_DATE] as [UNIT_AWARDED_DATE]
			  , [Marks1].[UNITOUTCOME_STATUS]
			  , [Marks1].[Link_Status] as [AWARD_STATUS]
			  , [Asmt].[assessment_count]
			  , [Asmt].[result_count]
			  , [Asmt].[results]
			  , convert(varchar(2000), [Teacher].[FORENAME]+' '+[Teacher].[SURNAME]) as [Teacher]
			  , [Marks1].[PROD_PROG_CODE]
			  , [Marks1].[PROD_PROG_REASON]
			  , [Marks1].[UNIT_PROGRESS_REASON]
			  , [Marks1].[uio_id]
			  , [Marks1].[sloc_location_code]
			  , [P].[person_code]
			  , [Marks1].[course_id]
			  , [Marks1].[Claim_Status]
			  , isNull(sp.[SkillsPointName], 'Unknown') as [SkillsPoint]
			  , '' as [Teachers]
			  , '' as [ADLoginAccounts]
			  ,cal.[InstituteID]
			into [#marks]
			from [#Marks1] as [Marks1]
				 left outer join [#Asmt] as [asmt] on [Asmt].[pu_id] = [Marks1].[pu_id]
				 inner join [EDW].[OneEBS_EBS_0165_PEOPLE] as [P] on [Marks1].[per_person_code] = [P].[Person_Code]
				 left outer join [EDW].[OneEBS_EBS_0165_GRADING_SCHEMES] as [gs] on [Marks1].[grading_scheme_id] = [GS].[ID]
				 left join [EDW].[OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL] as [pus_course] on [Marks1].[course_id] = [pus_course].[PEOPLE_UNITS_ID] and [Marks1].[special_details_course] = 'Y'
				 inner join [EDW].[OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES] as [uio_course] on [Marks1].[course_uioid] = [uio_course].[uio_id]
				 left join [compliance].[AvetmissLocation] as cal on cal.LocationCode = [uio_course].SLOC_LOCATION_CODE
				 left join [EDW].[OneEBS_EBS_0165_ORGANISATION_UNITS] as [org] on [org].[organisation_code] = [uio_course].[owning_organisation]
				 left join [EDW].[OneEBS_EBS_0165_ORGANISATION_UNITS] as [fac] on [fac].[organisation_code] = [uio_course].[offering_organisation]
				 left join [EDW].[OneEBS_EBS_0165_UNIT_INSTANCES] as [ui_course] on [uio_course].[fes_uins_instance_code] = [ui_course].[fes_unit_instance_code]
				 left join [EDW].[OneEBS_EBS_0165_LOCATIONS] as [lo] on [lo].[location_code] = [Marks1].[sloc_location_code]
				 left join [EDW].[OneEBS_EBS_0165_PEOPLE] as [teacher] on [Marks1].[Unit_Contact_Id] = [Teacher].[PERSON_CODE]
				 --below 2 joins have been added as suggested by Lauren
				 inner join [EDW].[OneEBS_EBS_0165_UNIT_INSTANCES] UI ON UI.FES_UNIT_INSTANCE_CODE = [uio_course].FES_UINS_INSTANCE_CODE
				 inner join [EDW].reference_course_skills_point csp on (ui.NATIONAL_COURSE_CODE = csp.NationalCourseCode OR ui.FES_UNIT_INSTANCE_CODE = csp.NationalCourseCode)
				 inner join [edw].[reference_skills_point] sp on csp.SkillsPointID = sp.SkillsPointID
				 --left join [EDW].[ODS_RefData].[SkillsPointMapping] as [sp] on [ui_course].[fes_unit_instance_code] = [sp].[fes_unit_instance_code]
			where not ( IsNull([Marks1].[Final_Grade], 'ZZZZZ') in ( 'AC', 'NC', 'WN' )
					   and IsNull([asmt].[result_count], 0) < IsNull([asmt].[assessment_count], 0)
					 );
			
			-- ET: 00:00:09

			
			truncate table [edw].[UMD_TempTeachersMarks];

			insert into [edw].[UMD_TempTeachersMarks]
			select 
				[Faculty]
			  , [College]
			  , [Teaching_Section]
			  , [YEAR/PERIOD]
			  , [First_Name]
			  , [Last_Name]
			  , [cid]
			  , [pu_id]
			  , [PROD_START_DT]
			  , [PROD_END_DT]
			  , [PROD_CODE]
			  , [PROD_NAME]
			  , [PROD_CALOCC]
			  , [LEARNER_NO]
			  , [PRODUCT_OFFER_TYPE]
			  , [unit_code]
			  , [unit_calocc]
			  , [Unit_type]
			  , [Unit_Prog_Code]
			  , [unit_start_dt]
			  , [unit_end_dt]
			  , [final_outcome]
			  , [Final_Grade]
			  , [GRADE_CODE]
			  , [GRADING_CODE_DESC]
			  , [UNIT_AWARDED_DATE]
			  , [UnitOutcome_Status]
			  , [AWARD_STATUS]
			  , [assessment_count]
			  , [result_count]
			  , [results]
			  , [Marks].[Teachers]
			  , case [Marks].[InstituteID]
					when '0127' then 'TAFE Digital'
					when '0158' then 'Sydney Region'
					when '0160' then 'North Region'
					when '0161' then 'Western Sydney'
					when '0162' then 'Western Sydney'
					when '0163' then 'West Region'
					when '0164' then 'South Region'
					when '0165' then 'Sydney Region'
					when '0166' then 'West Region'
					when '0167' then 'North Region'
					when '0168' then 'South Region'
				end as [Region]
			  , case [Marks].[InstituteID]
					when '0127' then 'OTEN'
					when '0158' then 'North Sydney'
					when '0160' then 'North Coast'
					when '0161' then 'South Western Sydney'
					when '0162' then 'Western Sydney'
					when '0163' then 'New England'
					when '0164' then 'Riverina'
					when '0165' then 'Sydney'
					when '0166' then 'Western'
					when '0167' then 'Hunter'
					when '0168' then 'Illawarra'
				end as [RTO]
			  , '1' as [EReportingPriority]
			  , case when c.Cnt > 0 then 'CLAIMABLE' else 'NON-CLAIMABLE' end as 'SWSI CLAIMABLE'
			  , null as [1220 Only]
			  , [Claim_Status] as [CLAIM_STATUS]
			  , [Prod_Prog_Code] as [Prod_Prog_Reason]
			  , [UNIT_PROGRESS_REASON]
			  , [uio_id]
			  , [SLOC_LOCATION_CODE]
			  , [PERSON_CODE]
			  , [course_id]
			  , [marks].[SkillsPoint]
			  , [marks].[ADLoginAccounts]
			  , @vRefreshTimestamp as [InsertedDate]
			from [#Marks] as [Marks]
			left join [#claimable] c on 
								c.[fes_uins_instance_code] = [Marks].[PROD_CODE]
							and c.[calocc_occurrence_code] = [Marks].[PROD_CALOCC]
							and c.start_date_year = case when year([Marks].[PROD_START_DT]) <= 2016 then '2016' else year([Marks].[PROD_START_DT]) end;

			-- ET: 00:02:48

			----------------------------------------------------------------------------------------------------
			-- Begin Transaction.
			----------------------------------------------------------------------------------------------------
			
			begin transaction;

			----------------------------------------------------------------------------------------------------
			-- Populate UMD.FactUnenteredMarks...
			----------------------------------------------------------------------------------------------------
			truncate table [edw].[UMD_FactUnenteredMarks];

			insert into [edw].[UMD_FactUnenteredMarks] ( 
		        [SkillsTeam]
			  , [College]
			  , [TeachingSection]
			  , [CourseProductDescription]
			  , [CourseProductCode]
			  , [CourseCaloccCode]
			  , [CourseProgressCode]
			  , [CourseProgressReason]
			  , [CourseStartDate]
			  , [CourseEndDate]
			  , [CourseOfferType]
			  , [YearPeriod]
			  , [eReportingPriority]
			  , [SWSiClaimable]
			  , [CId]
			  , [ClaimStatus]
			  , [1220Only]
			  , [PUId]
			  , [LearnerNumber]
			  , [FirstName]
			  , [LastName]
			  , [UnitProductCode]
			  , [UnitCaloccCode]
			  , [UnitType]
			  , [UnitProgressCode]
			  , [UnitProgressReason]
			  , [UnitStartDate]
			  , [UnitEndDate]
			  , [FinalOutcome]
			  , [FinalGrade]
			  , [GradeCode]
			  , [GradingCodeDescription]
			  , [UnitAwardedDate]
			  , [UnitOutcomeStatus]
			  , [AwardStatus]
			  , [AssessmentCount]
			  , [ResultCount]
			  , [Results]
			  , [TeachersAssigned]
			  , [Teacher]
			  , [Region]
			  , [RTOName]
			  , [DeliveryLocationDescription]
			  , [Latitude]
			  , [Longitude]
			  , [UnitEndDateAgeing]
			  , [CourseFundingSource]
			  , [TeacherADUserName] ) 
			select 
		        isNull([SkillsPoint], 'Unknown')
			  , [College]
			  , isNull([Teaching_Section], 'Unknown')
			  , [PROD_NAME]
			  , [PROD_CODE]
			  , [PROD_CALOCC]
			  , [Prod_Prog_Reason]
			  , null as [CourseProgressReason]
			  , [PROD_START_DT]
			  , [PROD_END_DT]
			  , [PRODUCT_OFFER_TYPE]
			  , [YEAR/PERIOD]
			  , [EReportingPriority]
			  , [SWSI CLAIMABLE]
			  , [cid]
			  , [CLAIM_STATUS]
			  , [1220 Only]
			  , [pu_id]
			  , [LEARNER_NO]
			  , [First_Name]
			  , [Last_Name]
			  , [unit_code]
			  , [unit_calocc]
			  , [Unit_type]
			  , [Unit_Prog_Code]
			  , [UNIT_PROGRESS_REASON]
			  , [unit_start_dt]
			  , [unit_end_dt]
			  , [final_outcome]
			  , [Final_Grade]
			  , [GRADE_CODE]
			  , [GRADING_CODE_DESC]
			  , [UNIT_AWARDED_DATE]
			  , [UnitOutcome_Status]
			  , [AWARD_STATUS]
			  , [assessment_count]
			  , [result_count]
			  , [results]
			  , isNull(null, 'Unknown') as [TeachersAssigned]
			  , isNull(left([Teacher], 2000), 'Unknown') as [Teacher]
			  , [Region]
			  , [RTO] as [RTOName]
			  , [SLOC_LOCATION_CODE]
			  , null as [Latitude]
			  , null as [Longitude]
			  , datediff([dd], [Unit_End_Dt], @vRefreshTimestamp) as [UnitEndDateAgeing]
			  , isNull(null, 'Unknown') as [CourseFundingSource]
			  , [ADLogin]
			from [edw].[UMD_TempTeachersMarks]

			-- ET: 00:00:01
			
			-- Aggregated 'Summary' data (yearly); for current year, aggregate up until today...

			delete from [edw].[UMD_DimSkillsPoint]
			where [AsAtDate] between convert(date, convert(varchar, year(@vRefreshTimestamp))+'-01-01') and convert(date, @vRefreshTimestamp);

			-- Insert SCD data into UMD.DimSkillsPoint table only for today (@RefreshTimestamp) aggregating Unentered Marks counts per Skillspoint for each year...
			insert into [edw].[UMD_DimSkillsPoint] (Skillpoint, AsAtDate, Year, Count)
			select 
				[SkillsTeam] as [Skillpoint]
			  , convert(date, @vRefreshTimestamp) as [AsAtDate]
			  , year([UnitEndDate]) as [Year]
			  , count(1) as Count
			from [edw].[UMD_FactUnenteredMarks]
			group by [SkillsTeam], year([UnitEndDate])
			
			commit transaction;

			--------------------
			Cleanup:
			--------------------
			if object_id('tempdb..#asmt') is not null drop table [#asmt]
			if object_id('tempdb..#marks') is not null drop table [#marks]
			if object_id('tempdb..#marks1') is not null drop table [#marks1]
			if object_id('tempdb..#claimable') is not null drop table [#claimable]

	End Try
	Begin Catch
	if @@tranCount > 0
		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)

	End Catch;
 END