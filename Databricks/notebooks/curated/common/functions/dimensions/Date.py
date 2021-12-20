# Databricks notebook source
# MAGIC %pip install fiscalyear

# COMMAND ----------

###########################################################################################################################
# Function: getDate
#  GETS Date DIMENSION 
# Returns:
#  Dataframe of transformed dates
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
import fiscalyear
from fiscalyear import *
from math import ceil
from datetime import date, datetime, timedelta
import time
from pyspark.sql.types import DateType, IntegerType, BooleanType

def getDate():
    def weekOfMonth(dt):
        """ Returns the week of the month for the specified date.
        """
        first_day = dt.replace(day=1)

        dom = dt.day
        adjusted_dom = dom + (1 + first_day.weekday()) % 7

        return int(ceil(adjusted_dom/7.0))

    def weekDates(dt,req='Start'):
        """ Returns the start date (Monday) or end date (Sunday) of the week a date falls in
        """
        req = req.lower()

        assert req in ['start','end'], "Second parameter must be 'start' or 'end'"

        if req == 'start':
            return dt - timedelta(days=dt.weekday())
        else:
            return dt + timedelta(days=6 - dt.weekday())    

    def monthOfQuarter(dt):
        """ Returns the month of in the quarter for the specified date.
        """
        return int(dt.month - ((dt.month-1)//3) * 3)

    def quarterDates(dt,req='Start'):
        """ Returns the start date or end date of the quarter a date falls in
        """
        req = req.lower()

        assert req in ['start','end', 'week'], "Second parameter must be 'start', 'end' or 'week'"

        quarter = (dt.month-1)//3+1 

        if req == 'start':
            return datetime(dt.year, 3 * quarter - 2, 1)
        elif req == 'end':
            return datetime(dt.year, (3 * quarter)%12+1, 1) + timedelta(days=-1)
        elif req == 'week':
            return ((dt - quarterDates(dt,'start').date()).days + 1)//7 + 1

    def fiscalDates(dt,req='year'):
        """ Returns components of the financial year for the specified date.
        """
        req = req.lower()

        assert req in ['day', 'week', 'month', 'quarter', 'year','start','end'], "Second parameter must be 'day', 'week', month', 'quarter', 'year', start' or 'end'"

        fiscalyear.setup_fiscal_calendar(start_month=7)
        fy = FiscalDate.fromtimestamp(time.mktime(dt.timetuple()))

        if req == 'year':
            return fy.fiscal_year
        elif req == 'start':
            return datetime.strptime(str(FiscalYear(fy.fiscal_year).start).split(' ')[0],'%Y-%m-%d')
        elif req == 'end':
            return datetime.strptime(str(FiscalYear(fy.fiscal_year).end).split(' ')[0],'%Y-%m-%d')
        elif req == 'day':
            return fy.fiscal_day
        elif req == 'month':
            return fy.fiscal_month
        elif req == 'quarter':
            return fy.fiscal_quarter
        elif req == 'week':
            day = FiscalYear(fy.fiscal_year).start.weekday()
            #week counting starts on the first Monday on or after 1/7
            if day == 0:
                firstMonday = FiscalYear(fy.fiscal_year).start
            else:
                firstMonday = FiscalYear(fy.fiscal_year).start + timedelta(days=7-day)

            if str(dt) < str(firstMonday)[:10]:
                #should the week be 52 or 53? Don't think this works well. easier to find by running a query on results
                #Most years have 52 weeks, but if the year starts on a Thursday or is a leap year that starts on a Wednesday, that particular year will have 53 numbered weeks

                if FiscalYear(fy.fiscal_year - 1).start.weekday() == 3 or (FiscalYear(fy.fiscal_year - 1).start.weekday() == 2 and (FiscalYear(fy.fiscal_year - 1).end - FiscalYear(fy.fiscal_year - 1).start).days + 1 == 366):
                    week = 53
                else:
                    week = 52
            else:
                week = int((((dt - timedelta(days=day)) - datetime.strptime(str(FiscalYear(fy.fiscal_year).start)[:10],'%Y-%m-%d').date()).days - 1) / 7) + 1
            return week
    
    spark.udf.register("weekOfMonth", weekOfMonth, IntegerType())
    spark.udf.register("weekDates", weekDates, DateType())
    spark.udf.register("quarterDates_Date", quarterDates, DateType())
    spark.udf.register("quarterDates_Int", quarterDates, IntegerType())
    spark.udf.register("monthOfQuarter", monthOfQuarter, IntegerType())
    spark.udf.register("fiscalDates_Date", fiscalDates, DateType())
    spark.udf.register("fiscalDates_Int", fiscalDates, IntegerType())
    #3.JOIN TABLES  


    #4.UNION TABLES

    #5.SELECT / TRANSFORM
    df = spark.sql(f"SELECT  \
                          calendarDate \
                          ,date_format(calendarDate, 'EEEE') as dayName \
                          ,date_format(calendarDate, 'MMMM') as monthName \
                          ,calendarYear \
                          ,dayOfWeek \
                          ,dayOfMonth \
                          ,dayOfYear \
                          ,weekOfMonth(calendarDate) as weekOfMonth \
                          ,weekOfYear \
                          ,monthOfYear \
                          ,quarterDates_Int(calendarDate,'week') as weekOfQuarter \
                          ,monthOfQuarter(calendarDate) as monthOfQuarter \
                          ,quarterOfYear \
                          ,halfOfYear \
                          ,weekDates(calendarDate,'start') as weekStartDate \
                          ,weekDates(calendarDate,'end') as weekEndDate \
                          ,monthStartDate \
                          ,monthEndDate \
                          ,quarterDates_Date(calendarDate,'start') as quarterStartDate \
                          ,quarterDates_Date(calendarDate,'end') as quarterEndDate \
                          ,cast(cast(calendarYear as string)||'-01-01' as date) as yearStartDate \
                          ,cast(cast(calendarYear as string)||'-12-31' as date) as yearEndDate \
                          ,case when dayOfWeek < 6 then True else False end as isWeekDayFlag \
                          ,fiscalDates_Int(calendarDate,'year') as financialYear \
                          ,fiscalDates_Date(calendarDate,'start') as financialYearStartDate \
                          ,fiscalDates_Date(calendarDate,'end') as financialYearEndDate \
                          ,fiscalDates_Int(calendarDate,'day') as dayOfFinancialYear \
                          ,fiscalDates_Int(calendarDate,'week') as weekOfFinancialYear \
                          ,fiscalDates_Int(calendarDate,'month') as monthOfFinancialYear \
                          ,fiscalDates_Int(calendarDate,'quarter') as quarterOfFinacialYear \
                          ,case when fiscalDates_Int(calendarDate,'quarter') < 3 then 2 else 1 end as halfOfFinacialYear \
                          from {ADS_DATABASE_CLEANSED}.isu_scal_tt_date \
                       ")
    
    #6.Apply schema definition
    newSchema = StructType([
                            StructField("calendarDate", DateType(), False),
                            StructField("dayName", StringType(), False),
                            StructField("monthName", StringType(), False),
                            StructField("calendarYear", IntegerType(), False),
                            StructField("dayOfWeek", IntegerType(), False),
                            StructField("dayOfMonth", IntegerType(), False),
                            StructField("dayOfYear", IntegerType(), False),
                            StructField("weekOfMonth", IntegerType(), False),
                            StructField("weekOfYear", IntegerType(), False),
                            StructField("monthOfYear", IntegerType(), False),
                            StructField("weekOfQuarter", IntegerType(), False),
                            StructField("monthOfQuarter", IntegerType(), False),
                            StructField("quarterOfYear", IntegerType(), False),
                            StructField("halfOfYear", IntegerType(), False),
                            StructField("weekStartDate", DateType(), False),
                            StructField("weekEndDate", DateType(), False),
                            StructField("monthStartDate", DateType(), False),
                            StructField("monthEndDate", DateType(), False),
                            StructField("quarterStartDate", DateType(), False),
                            StructField("quarterEndDate", DateType(), False),
                            StructField("yearStartDate", DateType(), False),
                            StructField("yearEndDate", DateType(), False),
                            StructField("isWeekDayFlag", BooleanType(), False),
                            StructField("financialYear", IntegerType(), False),
                            StructField("financialYearStartDate", DateType(), False),
                            StructField("financialYearEndDate", DateType(), True),
                            StructField("dayOfFinancialYear", IntegerType(), False),
                            StructField("weekOfFinancialYear", IntegerType(), False),
                            StructField("monthOfFinancialYear", IntegerType(), False),
                            StructField("quarterOfFinancialYear", IntegerType(), False),
                            StructField("halfOfFinancialYear", IntegerType(), False)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df

