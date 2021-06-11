CREATE proc [CTL].[sp_getFullName] (@fileName varchar(255), @Grain varchar(30))
as
begin
	select @fileName + '/' + [CTL].[udf_getFileDateHierarchy](@Grain) + '/' + @fileName + '.csv.gz' FileName
end