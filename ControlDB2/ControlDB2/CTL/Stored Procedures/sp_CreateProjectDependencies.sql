

--Exec CTL.sp_CreateProjectDependencies 'XFORM','FEMS,EBS,Excel'

CREATE Proc [CTL].[sp_CreateProjectDependencies]
@ProjectName varchar(100),
@Dependencies varchar(250)
As

Declare @Project int
Declare @Error varchar(1000)
Declare @ProjectList varchar(25) = ''
Declare @SQL varchar(2000)

--Get project id
Set @project = (select ProjectId from CTL.Project where [Name] = @ProjectName)

Set @error = 'Invalid project name: ' + @ProjectName
If @project is null RaisError(@error,16,1)


Select @ProjectList += cast(p.ProjectId as varchar(5)) + ','  from string_split (@Dependencies,',') as a left join [CTL].Project as p on p.Name = a.value

Set @error = 'Invalid project name in dependency list. Ensure correct values are used.'
If @ProjectList is null RaisError(@error,16,1)

Set @ProjectList = SUBSTRING(@ProjectList,0,len(@ProjectList))

Begin Try
Begin Transaction

	Update CTL.Process set ProjectDependency = @ProjectList where ProjectId = @Project

	Commit

End Try
Begin Catch

	Rollback
	Declare @err_num  int = @@ERROR
	Declare @err_desc varchar(500) = ERROR_MESSAGE()
	raiserror(@err_desc,@err_num,1)

End Catch