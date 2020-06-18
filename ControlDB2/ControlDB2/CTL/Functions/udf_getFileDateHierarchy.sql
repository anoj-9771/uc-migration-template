-- =============================================
-- Author:		Willem Van Aswegen
-- Create date: 01/01/2019
-- Description:	Function generate format herarchy based on grain
-- =============================================

CREATE     FUNCTION [CTL].[udf_getFileDateHierarchy] (@Grain varchar(20), @ExecutionTime datetime2)
RETURNS varchar(30)
AS
BEGIN

return
(
	select Case @Grain
				WHEN 'Day' then FORMAT(@ExecutionTime,N'/yyyy/MM/dd/')
				WHEN 'Month' then FORMAT(@ExecutionTime,N'/yyyy/MM/')
				WHEN 'Year' then FORMAT(@ExecutionTime,N'/yyyy/')
				WHEN 'Hour' then FORMAT(@ExecutionTime,N'/yyyy/MM/dd/HH/')
				WHEN 'Minute' then FORMAT(@ExecutionTime,N'/yyyy/MM/dd/HH/mm/')
				WHEN 'Second' then FORMAT(@ExecutionTime,N'/yyyy/MM/dd/HH/mm/ss/')
				ELSE FORMAT(@ExecutionTime,N'/yyyy/MM/dd/')
		   End
)


END