-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Basic function to return items in split string 
-- =============================================

create       function [CTL].[udf_splitString](@input varchar(max), @separator varchar(5))
returns table
as 
	return (
		select row_number()over(order by (select 1)) [Key], value [Value]  from  STRING_SPLIT(@input,@separator)
	)