CREATE Procedure [CTL].[GetStageTableSQL] (@TargetId BigInt)
As
  Declare @Output table (command varchar(max))
  Declare @SQL varchar(max) = ''


  BEGIN
    If (Select Count(*)
          From CTL.ControlTarget ct
            Join CTL.ControlTargetColumns ctc
              On ct.TargetId = ctc.TargetId
         Where ct.TargetId = @TargetId) = 0
      BEGIN
        RAISERROR('Target table definition is not available.',16,1)
      END
    Else
      BEGIN
        Insert into @Output
        Select 'Exec (''DROP TABLE IF EXISTS ' + ct.TargetLocation + '.' + ct.TargetName + char(10) + char(13) + char(10) + char(13) + ''') Select 1'
          From CTL.ControlTarget ct
         Where ct.TargetId = @TargetId

        Select @SQL = 'Exec(''CREATE TABLE ' + ct.TargetLocation + '.' + ct.TargetName + ' ( '
          From CTL.ControlTarget ct
         Where ct.TargetId = @TargetId

        Select @SQL = @SQL + ColumnName + '  ' + DataType + Case When ctc.MaxLength = -1 then '(MAX)' 
                  When ctc.MaxLength = 0 then '' else '(' + Convert(varchar, ctc.MaxLength) + case when ctc.Precision = 0 then ')' else ', ' + convert(varchar, ctc.Precision) + ')' end end + ', '
          From CTL.ControlTargetColumns ctc
         Where ctc.TargetId = @TargetId
         
        Select @SQL = LEFT(@SQL, LEN(@SQL) - 1) + ' )'')' + char(10) + char(13) + char(10) + char(13) + ' Select 1'

        Insert into @Output Values (@SQL)

        Select * From @Output

      END

  END