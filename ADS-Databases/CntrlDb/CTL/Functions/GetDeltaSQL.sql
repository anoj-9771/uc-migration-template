CREATE FUNCTION [CTL].[GetDeltaSQL] (@TaskId BigInt)
Returns Varchar(MAX)
AS
BEGIN
Declare @SQL Varchar(Max) = 'Select * ',
		@TableName Varchar(255) = ' From ',
		@Where Varchar(2000) = ' Where '

Declare @Columns Table
(
  ColumnName Varchar(255),
  SourceSQL Varchar(2000)
)

Declare @Watermakrs Table
(
  Watermark Varchar(255)
)

If (Select Count(*)
      From [CTL].[ControlSourceColumns] col
        Join [CTL].[ControlSource] cs
          On col.SourceTableId = cs.SourceId
        Join [CTL].[ControlTasks] ct
          On cs.SourceId = ct.SourceId
     Where ct.TaskId = @TaskId) = 0
  BEGIN
    Select @SQL = 'Select * '
  END
Else
  BEGIN
    Select @SQL = 'Select '
    Select @SQL = @SQL + Case When Len(col.ColumnQuery) = 0 then col.ColumnName else col.ColumnQuery end + ', '
      From [CTL].[ControlSourceColumns] col
        Join [CTL].[ControlSource] cs
          On col.SourceTableId = cs.SourceId
        Join [CTL].ControlTasks ct
          On cs.SourceId = ct.SourceId
     Where ct.TaskId = @TaskId
    Select @SQL = Left(@SQL, Len(@SQL) - 1)
  END

	Select @TableName = @TableName + (Select SourceLocation From CTL.ControlSource cs Join CTL.ControlTasks ct ON cs.SourceId = ct.SourceId  Where ct.TaskId = @TaskId)

	Insert Into @Columns
		Select m.SourceColumn, m.SourceSQL
			From [CTL].[ControlWatermark] m
				Join [CTL].[ControlSource] cs
				On m.ControlSourceId = cs.SourceId
			Join [CTL].[ControlTasks] ct
				On cs.SourceId = ct.SourceId
			Where ct.TaskId = @TaskId

	Insert Into @Watermakrs
	Select value
	  From string_split((
					Select m.Watermarks
					  From [CTL].[ControlWatermark] m
					    Join [CTL].[ControlSource] cs
						  On m.ControlSourceId = cs.SourceId
						Join [CTL].[ControlTasks] ct
						  On cs.SourceId = ct.SourceId
					 Where ct.TaskId = @TaskId), '|')

	While (Select Count(*) From @Columns) > 0
	  BEGIN
		--Updated by Rahul Agrawal on 28-Jan-2020. Updated the condition to > from >=. The >= operator loads the last watermark data over and over.
		Select @Where = @Where  + (Select Top 1 SourceSQL From @Columns) + ' > ''' + (Select Top 1 Watermark From @Watermakrs) + ''' AND '
		Delete Top(1) From @Columns
		Delete Top(1) From @Watermakrs
	  END

	Select @Where = LEFT(@Where, Len(@Where) - 4)

	Select @SQL = @SQL + @TableName + @Where

	Return @SQL 
END