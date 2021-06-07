CREATE proc [CTL].[GetStorageFileName] (
	@FolderName varchar(255)
	,@FileName varchar(255)
	,@Grain varchar(30)
	,@DeltaExtract bit
	,@TargetType varchar(255)
	,@Compressed bit = 1)
as
begin
	
	set @FolderName = LOWER(@FolderName)
	declare @DataType varchar(10)

	IF @TargetType = 'BLOB Storage (csv)' SET @DataType = 'csv'
	ELSE IF @TargetType = 'BLOB Storage (json)' SET @DataType = 'json'
	ELSE IF @TargetType = 'BLOB Storage (parquet)' SET @DataType = 'parquet'
	ELSE SET @DataType = 'csv'

	declare @blobfoldername varchar(1000)
	set @blobfoldername = @FolderName + '/' + @DataType + '/' + [CTL].[udf_GetFileDateHierarchy](@Grain) + '/'

	declare @blobfilename varchar(1000)
	IF @DeltaExtract = 1
	BEGIN
		set @blobfilename = @FileName + '_' + format([CTL].[udf_GetDateLocalTZ](),N'yyyy-MM-dd_HHmmss_fff') + '.' + @DataType
	END
	ELSE
	BEGIN
		set @blobfilename = @FileName + '_' + format([CTL].[udf_GetDateLocalTZ](),N'yyyy-MM-dd') + '.' + @DataType
		--Sometimes when the file with the same name is overwritten, Databricks does not read any record
		--Trying to check if having a unique name helps
		set @blobfilename = @FileName + '_' + format([CTL].[udf_GetDateLocalTZ](),N'yyyy-MM-dd_HHmmss_fff') + '.' + @DataType
	END

	IF @Compressed = 1 SET @blobfilename = @blobfilename + '.gz'


	SELECT @blobfoldername AS StorageFolder, @blobfilename AS StorageFile

end