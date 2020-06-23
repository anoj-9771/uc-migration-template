-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Basic function to split string 
-- =============================================

CREATE   function [CTL].[udf_split](@input varchar(max), @separator varchar(5), @position int)
returns varchar(max)
as 
begin
	return (
		select case when (
			select count(*)  from  STRING_SPLIT(@input,@separator)
			) = 0  then (
				select a.value from (
				select row_number()over(order by (select 1)) RowNum, value from  STRING_SPLIT(@input,@separator)
				)a
				where a.RowNum = 1
			) else (
				select a.value from (
				select row_number()over(order by (select 1)) RowNum, value from  STRING_SPLIT(@input,@separator)
				)a
				where a.RowNum = @position + 1
			) end
	)
end