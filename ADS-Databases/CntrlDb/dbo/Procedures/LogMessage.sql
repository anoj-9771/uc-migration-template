


CREATE PROCEDURE [dbo].[LogMessage] 
	@ID INT = NULL,
	@IsTransform BIT = 0,
	@ActivityType VARCHAR(100) = NULL,
	@Message VARCHAR(MAX)
AS
BEGIN
	IF @IsTransform = 1
	BEGIN
		INSERT INTO [dbo].[Log] 
		([TransformStatusID]
		,[ActivityType]
		,[Message]
		,[CreatedDTS]) 
		SELECT @ID
		,@ActivityType
		,@Message
		,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time')
	END
	ELSE
	BEGIN
		INSERT INTO [dbo].[Log] 
		([ExtractLoadStatusID]
		,[ActivityType]
		,[Message]
		,[CreatedDTS]) 
		SELECT @ID
		,@ActivityType
		,@Message
		,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time')
	END

	declare @logId int, @recipientList varchar(1000) = null

    select @logId = @@IDENTITY

    select @recipientList = value
    from dbo.config
    where keyGroup = 'adfEmailAlerting'
    and [key] = 'recipientList'

    if @recipientList is not null and exists(
        select 1 from dbo.log
        where id = @logId and json_value(message,'$.ExecutionStatus') = 'Fail'
    )
    begin
    select 
        LogId = l.Id
        ,s.BatchID
        ,s.SystemCode
        ,m.DestinationTableName
        ,l.ExtractLoadStatusID
        ,l.ActivityType
        ,ErrorMessage = REPLACE(l.Message,'"','\"') -- add another \ to allow parsing through the logic app json payload
        ,RecipientList = @recipientList
        ,Alert = 1
    from dbo.log l
    left join dbo.ExtractLoadStatus s on l.ExtractLoadStatusID = s.ID
    left join dbo.ExtractLoadManifest m on s.SourceID = m.SourceID
    where l.id = @logId
    end
    else begin
        select Alert = 0
    end
END
GO


