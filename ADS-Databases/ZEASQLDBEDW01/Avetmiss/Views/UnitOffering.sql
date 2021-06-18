





Create View [Avetmiss].[UnitOffering]

as
 SELECT [AvetmissUnitOfferingSK]
      ,[UnitOfferingID]
      ,[UnitOfferingCode]
      ,[AttendanceCode]
      ,[AttendanceModeName]
      ,[AttendanceModeDescription]
      ,[CaloccOccurrenceCode]
      ,[MaximumHours]
      ,[OfferingCode]
      ,[OfferingCostCentre]
      ,[OfferingCostCentreCode]
      ,[OfferingDeliveryLocation]
      ,[OfferingDeliveryMode]
      ,[DeliveryModeName]
      ,[DeliveryModeDescription]
      ,[AvetmissDeliveryModeID]
      ,[OfferingEnrolmentLocation]
      ,[OfferingFundingSource]
      ,[OfferingStatus]
	  ,os.OfferingStatusDescription
      ,[OfferingType]
      ,[OfferingStartDate]
      ,[OfferingEndDate]
      ,[AvetmissDeliveryModeCode]
      ,[TeachingSectionCode]
      ,[TeachingSectionDescription]
      ,[Faculty]
      ,[FacultyDescription]
      ,[DeliveryHours]
      ,[PredominantDeliveryMode]
      ,[UnitCode]
      ,o.[_DLCuratedZoneTimeStamp]
      ,o.[_RecordStart]
      ,o.[_RecordEnd]
      ,o.[_RecordDeleted]
      ,o.[_RecordCurrent]
  FROM [compliance].[AvetmissUnitOffering] o
  Left join [reference].[OfferingStatus] os on o.OfferingStatus = os.OfferingStatusCode and os._RecordDeleted = 0 and os._RecordCurrent = 1