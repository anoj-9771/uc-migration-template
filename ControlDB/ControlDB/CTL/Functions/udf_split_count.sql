-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Basic function to count items in split string 
-- =============================================

create     function [CTL].[udf_split_count](@input varchar(max), @separator varchar(5))
returns int
as 
begin
	return (
		select count(*)  from  STRING_SPLIT(@input,@separator)
	)
end