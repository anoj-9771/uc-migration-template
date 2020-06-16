

--EXEC CTL.sp_CreateUpdateProcess 'a','aaa',1,1,1,'Day',1

CREATE   PROC [CTL].[sp_CreateUpdateProcess]
	@ProcessName	varchar(150),
	@Description	varchar(1000),
	@ProjectId		int,
	@Enabled		int,
	@Priority		int,
	@Grain			varchar(50),
	@TypeId			int
AS

   Declare @processCheck int
   Select @processCheck = Count(*) from CTL.Process where [Name] = TRIM(@ProcessName) and ProjectId = @ProjectId

   Begin Try
   Begin Transaction

		if @processCheck = 0
		Begin

				INSERT INTO [CTL].[Process] ([Name],[Description],ProjectId,[Enabled],[Priority],Grain,Typeid,ProcessDependency,ProjectDependency)
				Values (TRIM(@ProcessName), TRIM(@Description), @ProjectId, @Enabled, @Priority, TRIM(@Grain), @TypeId,'','')	

		End
		Else
		Begin

				Update CTL.Process
				Set [Description] = TRIM(@Description),
				   [Enabled] = @Enabled,
				   [Priority] = @Priority,
				   [Grain] = TRIM(@Grain),
				   [TypeId] = @TypeId
				Where [Name] = TRIM(@ProcessName) and ProjectId = @ProjectId

		End

	Commit
End Try
Begin Catch

		Rollback
		Declare @err_num  int = @@ERROR
		Declare @err_desc varchar(500) = ERROR_MESSAGE()
		raiserror(@err_desc,@err_num,1)

End Catch