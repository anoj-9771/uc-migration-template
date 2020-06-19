-- =============================================
-- Author:	 Insight(Stephen Lundall)
-- Create date: 2020-02-11
-- Description:	Stored procedure to truncate table if exists
-- =============================================
CREATE    PROCEDURE dbo.usp_TruncateIfExists @TableName varchar(250) 
AS
BEGIN
	declare @query varchar(500) = 'TRUNCATE TABLE ' + @TableName
	--SET NOCOUNT ON;
	IF OBJECT_ID('tempdb..#' + @TableName  + '') IS NOT NULL
		BEGIN
			print N'Truncating table...'
			exec(@query)
		END
	ELSE
		BEGIN
			print N'Table ''' + @TableName +  ''' does not exist'
		END
END