




CREATE   view [OneEBS].[900_UI_Links] AS  Select * from edw.OneEBS_EBS_0900_UI_LINKS WHERE        (_RecordDeleted = 0) AND (_RecordCurrent = 1)