


create   view [ISDP].[SemesterDates]
as 


SELECT [SemesterYear]
      ,[SemesterNumber]
      ,[SemesterDescription] AS [SemesterName]
      ,[SemesterStartDate]
      ,[SemesterEndDate]
      ,CASE WHEN SemesterNumber = 1 then Concat('FY', YEAR([SemesterEndDate])) else Concat('FY',YEAR([SemesterEndDate])+1)  end as FinancialYear
	  ,[PlanStartDate] AS [PlanningOpenDate]
	  ,[PlanEndDate] AS [PlanningCloseDate]
	  ,[ReviewStartDate] AS [PlanningReviewOpenDate]
	  ,[ReviewEndDate] AS [PlanningReviewCloseDate]
	  ,[AdjustmentStartDate] AS [PlanningAdjustmentOpenDate]
	  ,[AdjustmentEndDate] AS [PlanningAdjustmentEndDate]
	  ,[SecondReviewStartDate] AS [SecondPlanningReviewOpenDate]
	  ,[SecondReviewEndDate] AS [SecondPlanningReviewCloseDate]
	  ,[SecondAdjustmentStartDate] AS [SecondPlanningAdjustmentOpenDate]
	  ,[SecondAdjustmentEndDate] AS [SecondPlanningAdjustmentEndDate]
FROM [reference].[Semester] A 
LEFT join [reference].[ServiceDeliveryPlanningDates] b ON A.SemesterID = B.SemesterID
WHERE SemesterYear BETWEEN 2019 AND 2024