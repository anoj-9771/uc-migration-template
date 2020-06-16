-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Basic function to return items in split string 
-- =============================================

CREATE       function [CTL].[udf_split_table](@input varchar(max), @separator varchar(5))
returns table
as 
	return (
		select value Dependents  from  STRING_SPLIT(@input,@separator)
	)