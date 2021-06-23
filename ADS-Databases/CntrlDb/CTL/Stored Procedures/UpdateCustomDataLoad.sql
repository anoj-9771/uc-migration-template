create procedure CTL.UpdateCustomDataLoad (
@TableName varchar(255),
@StartDate date)
AS

UPDATE CTL.CustomDataLoad SET ExtractionComplete = 1
WHERE TableName = @TableName
AND StartDate = @StartDate