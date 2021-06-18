

/*********************************************************************************************
-- ===========================================================================================
-- Description:	Populates the dimension table UMD.DimFunding from EDW ebs tables
-- Example:		EXEC UMD.USP_RefreshUMDDimFunding 

SELECT count(*) from UMD.DimFunding;
SELECT * from UMD.DimFunding;

-- Change History
Date	Modified By		Ticket No		Remarks

-- ===========================================================================================
**********************************************************************************************/

CREATE procedure [UMD].[USP_RefreshUMDDimFunding] 
as
	begin

		----------------------------
		-- SET environment variables
		----------------------------
		set nocount on; -- the count is not returned
		set xact_abort on; -- if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.

		begin try
		
			----------------------------------------------------------------------------------------------------
			-- Populate UMD.DimFunding...
			----------------------------------------------------------------------------------------------------

			if object_id('tempdb..#DimFunding') is not null drop table [#DimFunding]
			select [CourseFundingSourceCode]
				   , [CourseFundingSourceName]
				   , [CourseFundingSourceDescription]
			into [#DimFunding]
			from [edw].[UMD_DimFunding]
			where 1=2;
			--exec tempdb..sp_help '#DimFunding'

			with cte_DimFunding
			as 
			(
				select 
					[LOW_VALUE] as [CourseFundingSourceCode],
					[FES_SHORT_DESCRIPTION] as [CourseFundingSourceName],
					[FES_LONG_DESCRIPTION] as [CourseFundingSourceDescription]
				from [edw].[OneEBS_EBS_0165_VERIFIERS]
				where [RV_DOMAIN] = 'FUNDING'
				and [LOW_VALUE] <> '000'
			)
			insert into #DimFunding ([CourseFundingSourceCode], [CourseFundingSourceName], [CourseFundingSourceDescription])
			select distinct [CourseFundingSourceCode], [CourseFundingSourceName], [CourseFundingSourceDescription]
			from [cte_DimFunding];


			----------------------------------------------------------------------------------------------------
			-- Begin Transaction.
			----------------------------------------------------------------------------------------------------
			begin tran;

				truncate table [edw].[UMD_DimFunding];

				insert into [edw].[UMD_DimFunding] ( 
					[CourseFundingSourceCode]
				  , [CourseFundingSourceName]
				  , [CourseFundingSourceDescription] ) 
				select 
					[CourseFundingSourceCode]
				  , [CourseFundingSourceName]
				  , [CourseFundingSourceDescription]
				from [#DimFunding];

				if @@tranCount > 0
				begin
					commit tran;
				end;

			--------------------
			Cleanup:
			--------------------
			if object_id('tempdb..#DimFunding') is not null
			begin
				drop table [#DimFunding]
			end;
		end try
		begin catch
			if @@tranCount > 0
		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)
		end catch;
	end;