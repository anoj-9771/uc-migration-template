-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 02/09/2020
-- Description: Stored proc to order queue
--==============================================
CREATE    procedure [CTL].[sp_Dequeue_Stream]
AS
begin 
	declare @BatchSize int
	set @BatchSize = 10

	update top(@BatchSize) ctl.QueueMeta_Stream WITH (UPDLOCK, READPAST)
	SET Status = 1
	--OUTPUT inserted.QueueID, inserted.TaskId
	FROM ctl.QueueMeta_Stream qms
	WHERE Status = 0

	print N'Deque Complete'
end