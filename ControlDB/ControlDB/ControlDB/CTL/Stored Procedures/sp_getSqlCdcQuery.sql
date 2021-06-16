-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 02/09/2020
-- Description: Function used to generate sql query to run against a cdc enabled DB
-- use  'all update old' for update before and update after
-- =============================================

CREATE     proc [CTL].[sp_getSqlCdcQuery](@from_lsn nvarchar(1000) = null, @to_lsn nvarchar(1000),@tableName nvarchar(500), @Columns nvarchar(max))
as 
begin
	declare @SQL nvarchar(max)
	set @tableName = replace(@tableName, '.','_')	
	--SET @from_lsn =  '0x0000033E00003058014A'--convert(varchar(max), sys.fn_cdc_get_min_lsn('COD_tblCurrentInpatients'),1)


	--set @Columns = '[AreaHealthServiceCode], [AreaHealthRegion], [HospCode], [HospitalName], [AccountNumber], [UMRN], [EpisodeType], [EpisodeTypeDesc], [CareType], [CareTypeDesc], [AdmitType], [AdmitTypeDesc], [AdmitDateTime], [HospitalDivision], [HospitalLenghtOfStay], [LengthOfStay], [PatientType], [PatientTypeDesc], [PlannedDischargeDate], [PlannedDischargeStatus], [CurrentSpecialty], [CurrentSpecDesc], [CurrentWard], [CurrentRoom], [CurrentBed], [RoomBedDesc], [CurrentDrLastName], [CurrentDrFirstName], [Surname], [FirstName], [Gender], [PatientDOB], [CurrentAge], [RelegionDesc], [PatientSuburb], [PatientPostCode], [PatientClassCode], [PatientClass], [CatchmentSecondary1Hosp], [SiteUID], [COVID19Suspect], [COVID19Confirmed], [Result], [ResultDate], [LoadID], [HasMedAlert], [HasMicroAlert], [PrivacyIndicator]'
	if @from_lsn is null or @from_lsn = ''
	begin
		set @from_lsn = 'sys.fn_cdc_get_min_lsn(''' + @tableName + ''')'
		set @SQL = 'SELECT
				' + @Columns + '
				,''__'' + convert(varchar(100),ct.__$start_lsn, 1) __$start_lsn
				,ct.__$operation
				,CASE ct.__$operation
					WHEN 1 THEN ''D''
					WHEN 2 THEN ''I''
					WHEN 3 THEN ''UB''
					WHEN 4 THEN ''U''
				END AS Flag
				,lsn.tran_begin_time
				,lsn.tran_end_time
				,''__'' + convert(varchar(100),lsn.tran_id, 1) tran_id
			from 

			[cdc].[fn_cdc_get_all_changes_' + @tableName + ']

			(' + @from_lsn + ', @to_lsn,  N''all'') AS ct INNER JOIN

			cdc.lsn_time_mapping AS LSN ON ct.__$start_lsn = lsn.start_lsn
			order by __$start_lsn
			'
	end
	else 
	begin
		set @SQL = 'SELECT
				' + @Columns + '
				,''__'' + convert(varchar(100),ct.__$start_lsn, 1) __$start_lsn
				,ct.__$operation
				,CASE ct.__$operation
					WHEN 1 THEN ''D''
					WHEN 2 THEN ''I''
					WHEN 3 THEN ''UB''
					WHEN 4 THEN ''U''
				END AS Flag
				,lsn.tran_begin_time
				,lsn.tran_end_time
				,''__'' + convert(varchar(100),lsn.tran_id, 1) tran_id
			from 

			[cdc].[fn_cdc_get_all_changes_' + @tableName + ']

			(' + @from_lsn + ', @to_lsn, N''all'') AS ct INNER JOIN

			cdc.lsn_time_mapping AS LSN ON ct.__$start_lsn = lsn.start_lsn
			order by __$start_lsn
			'
	end
	select @SQL
end