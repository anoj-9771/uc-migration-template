
CREATE PROCEDURE [dbo].[spCreateCalendar]
AS
DECLARE @StartDate DATE = '20000101', @NumberOfYears INT = 50;


/*******************************************************************
Name : UTIL.uspCreateCalendar
Created by : Rahul Agrawal
Created on : 19-Jun-2017
Description : The SP is used to create the Calendar table. 
By Default the SP creates a calendar for 50 years based on the start date. Please update the parameters for changing the Start Date and length
The Calendar also has provision for Public Holidays. 
The Public holiday information can be uploaded using the SSIS package LOAD_HolidayList.dtsx
*******************************************************************
Change log
Please maintain the change log in the below section.
Updated On | Updated By | Update Details
(1) 
(2) 

*******************************************************************/

-- prevent set or regional settings from interfering with 
-- interpretation of dates / literals

SET DATEFIRST 7;
SET DATEFORMAT dmy;
SET LANGUAGE US_ENGLISH;

DECLARE @CutoffDate DATE = DATEADD(YEAR, @NumberOfYears, @StartDate);

-- this is just a holding table for intermediate calculations:
--DROP TABLE #dim

CREATE TABLE #dim
(
  [date]       DATE PRIMARY KEY, 
  [day]        AS DATEPART(DAY,      [date]),
  [month]      AS DATEPART(MONTH,    [date]),
  FirstOfMonth AS CONVERT(DATE, DATEADD(MONTH, DATEDIFF(MONTH, 0, [date]), 0)),
  [MonthName]  AS DATENAME(MONTH,    [date]),
  [week]       AS DATEPART(WEEK,     [date]),
  [ISOweek]    AS DATEPART(ISO_WEEK, [date]),
  [DayOfWeek]  AS DATEPART(WEEKDAY,  [date]),
  [quarter]    AS DATEPART(QUARTER,  [date]),
  [year]       AS DATEPART(YEAR,     [date]),
  FirstOfYear  AS CONVERT(DATE, DATEADD(YEAR,  DATEDIFF(YEAR,  0, [date]), 0)),
  Style112     AS CONVERT(CHAR(8),   [date], 112),
  Style101     AS CONVERT(CHAR(10),  [date], 101)
);

-- use the catalog views to generate as many rows as we need

INSERT #dim([date]) 
SELECT d
FROM
(
  SELECT d = DATEADD(DAY, rn - 1, @StartDate)
  FROM 
  (
    SELECT TOP (DATEDIFF(DAY, @StartDate, @CutoffDate)) 
      rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
    FROM sys.all_objects AS s1
    CROSS JOIN sys.all_objects AS s2
    -- on my system this would support > 5 million days
    ORDER BY s1.[object_id]
  ) AS x
) AS y;

IF OBJECT_ID('dbo.Calendar', 'U') IS NOT NULL 
    drop table dbo.Calendar;

CREATE TABLE dbo.Calendar
(
  DateKey				INT         NOT NULL PRIMARY KEY,
  [Date]				DATE        NOT NULL,
  [Day]					TINYINT     NOT NULL,
  DaySuffix				CHAR(2)     NOT NULL,
  [Weekday]				TINYINT     NOT NULL,
  WeekDayName			VARCHAR(10) NOT NULL,
  IsWeekend				BIT         NOT NULL,
  [DayOfYear]			SMALLINT    NOT NULL,
  WeekOfMonth			TINYINT     NOT NULL,
  WeekOfYear			TINYINT     NOT NULL,
  [Month]				TINYINT     NOT NULL,
  [MonthID]             VARCHAR(10) NOT NULL,
  [MonthName]			VARCHAR(10) NOT NULL,
  ShortMonthYearName	VARCHAR(10) NOT NULL,
  [Quarter]				TINYINT     NOT NULL,
  QuarterName			VARCHAR(6)  NOT NULL,
  HalfYearName			VARCHAR(6)	NOT NULL,
  [Year]				INT         NOT NULL,
  FirstDayOfWeek		DATE		NOT NULL,
  FirstDayOfMonth		DATE        NOT NULL,
  LastDayOfMonth		DATE        NOT NULL,
  FirstDayOfQuarter		DATE        NOT NULL,
  LastDayOfQuarter		DATE        NOT NULL,
  FirstDayOfYear		DATE        NOT NULL,
  LastDayOfYear			DATE        NOT NULL,
  FiscalYear			INT			NOT NULL,
  FiscalName			VARCHAR(7)	NOT NULL,
  FiscalQuarterName		VARCHAR(6)  NOT NULL,
  FiscalHalfYearName	VARCHAR(6)	NOT NULL,
  FiscalStartDate		DATE        NOT NULL,
  FiscalEndDate			DATE        NOT NULL,
  FiscalMonth			TINYINT     NOT NULL,
  FiscalMonthName		VARCHAR(10) NOT NULL,
);


INSERT dbo.Calendar WITH (TABLOCKX)
SELECT
  DateKey					=	CONVERT(INT, Style112),
  [Date]					=	[date],
  [Day]						=	CONVERT(TINYINT, [day]),
  DaySuffix					=	CONVERT(CHAR(2), CASE WHEN [day] / 10 = 1 THEN 'th' ELSE 
								  CASE RIGHT([day], 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
								  WHEN '3' THEN 'rd' ELSE 'th' END END),
  [Weekday]					=	CONVERT(TINYINT, [DayOfWeek]),
  [WeekDayName]				=	CONVERT(VARCHAR(10), DATENAME(WEEKDAY, [date])),
  [IsWeekend]				=	CONVERT(BIT, CASE WHEN [DayOfWeek] IN (1,7) THEN 1 ELSE 0 END),
  [DayOfYear]				=	CONVERT(SMALLINT, DATEPART(DAYOFYEAR, [date])),
  WeekOfMonth				=	CONVERT(TINYINT, DENSE_RANK() OVER (PARTITION BY [year], [month] ORDER BY [week])),
  WeekOfYear				=	CONVERT(TINYINT, [week]),
  [Month]					=	CONVERT(TINYINT, [month]),
  MONTHID					=	CONVERT(VARCHAR, YEAR) + RIGHT('0' + CONVERT(VARCHAR, [month]), 2),
  [MonthName]				=	CONVERT(VARCHAR(10), [MonthName]),
  ShortMonthYearName		=	CAST([YEAR] AS CHAR(4)) + '-' + LEFT([MonthName], 3),
  [Quarter]					=	CONVERT(TINYINT, [quarter]),
  QuarterName				=	'Q' + CONVERT(varchar, [quarter]), 
  HalfYearName				=	'H' + CASE WHEN [quarter] IN (1, 2) then '1' else '2' end,
  [Year]					=	[year],
  FirstDayOfWeek			=	DATEADD(DAY, 1-DATEPART(WEEKDAY, [date]), [date]),
  FirstDayOfMonth			=	FirstOfMonth,
  LastDayOfMonth			=	MAX([date]) OVER (PARTITION BY [year], [month]),
  FirstDayOfQuarter			=	MIN([date]) OVER (PARTITION BY [year], [quarter]),
  LastDayOfQuarter			=	MAX([date]) OVER (PARTITION BY [year], [quarter]),
  FirstDayOfYear			=	FirstOfYear,
  LastDayOfYear				=	MAX([date]) OVER (PARTITION BY [year]),
  FiscalYear				=	CASE WHEN [quarter] in (1) then [year] else [year] + 1 end,
  FiscalName				=	'FY-' + CONVERT(varchar, [year]),
  FiscalQuarterName			=	'Q' + CONVERT(varchar, [quarter]), 
  FiscalHalfYearName		=	'H' + CASE WHEN [quarter] IN (1, 2) then '1' else '2' end,
  FiscalStartDate			=	CONVERT(DATE, CONVERT(VARCHAR, [year]) + 'JAN 01'),
  FiscalEndDate				=	CONVERT(DATE, CONVERT(VARCHAR, [year]) + 'DEC 31'),
  FiscalMonth				=	[month],
  FiscalMonthName			=	CONVERT(VARCHAR(10), [MonthName])
/*
  FiscalYear				=	CASE WHEN [quarter] in (1) then [year] else [year] + 1 end,
  FiscalName				=	'FY-' + right(convert(varchar, CASE WHEN [quarter] in (1) then [year] else [year] + 1 end), 4),
  FiscalQuarterName			=	'Q' + CONVERT(VARCHAR, CASE WHEN [quarter] = 1 then 4 WHEN [quarter] = 2 then 1 WHEN [quarter] = 3 then 2 else 3 end), 
  FiscalHalfYearName		=	'H' + CASE WHEN [quarter] IN (1,4) then '2' else '1' end,
--  FiscalStartDate			=	DATEFROMPARTS(CASE WHEN [quarter] in (1,2) then [year]-1 else [year] end, 7,1),
  FiscalStartDate			=	CONVERT(DATE, CONVERT(VARCHAR, (CASE WHEN [quarter] in (1) then [year]-1 else [year] end)) + 'JULY 01'),
--  FiscalEndDate				=	DATEFROMPARTS(CASE WHEN [quarter] in (1,2) then [year] else [year] + 1 end, 6,30),
  FiscalEndDate				=	CONVERT(DATE, CONVERT(VARCHAR, (CASE WHEN [quarter] in (1) then [year] else [year] + 1 end)) + 'JUNE 30'),
  FiscalMonth				=	CASE WHEN [quarter] in (1,2) then [month] + 6 else [month] - 6 end,
  FiscalMonthName			=	CONVERT(VARCHAR(10), [MonthName])
*/
FROM #dim
OPTION (MAXDOP 1);