# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

# DBTITLE 1,Transaction Data - WPI Config
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}cleansed.ppm.eppmWpiConfig AS 
select distinct fiscalYear, valueType, type from 
(
  With cpi AS (
    select calenderYear, fiscalYear, valueType as rateType, wpiRate, row_number() over(partition by calenderYear order by calenderYear, fiscalYear) as rowNumber from {get_env()}cleansed.ppm.wpi
    where valueType <> 'CPI'  and (calenderYear <> '2027' or fiscalYear <> '2032' or wpiRate <> '0.8564')
  ),
  cpiBaseYear AS (
    select * from cpi where rowNumber = 1
  ),
  cpiWpi(
    select cpi.calenderYear, cpi.fiscalYear, cpi.rateType, cpi.wpiRate,wpi.wpiRate as wpiRateWPI,wpi.type,wpi.valueType from cpi
    join {get_env()}cleansed.ppm.zbwt_wpi wpi on cpi.fiscalYear = wpi.fiscalYear
  )
  select cpiWpi.fiscalYear,cpiBaseYear.calenderYear,cpiWpi.valueType,cpiWpi.type, case when cpiWpi.type = 'NOMINAL' then '1.000' else cpiWpi.wpiRate end as wpiRate from cpiWpi 
  join cpiBaseYear on cpiBaseYear.calenderYear = cpiWpi.calenderYear
)
""")

# COMMAND ----------

# DBTITLE 1,Transaction Data - COSP
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}cleansed.fin.eppmCospTranspose
AS
(
  WITH cospInter AS
  (select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '001' as postingPeriod,
          concat(fiscalYear,'001') as fiscalYearPeriod,
          totalValueTransactionCurrency1 as amountInTransactionCurrency,
          TotalValueControllingArea1 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency1<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '002' as postingPeriod,
          concat(fiscalYear,'002') as fiscalYearPeriod,
          totalValueTransactionCurrency2 as amountInTransactionCurrency,
          TotalValueControllingArea2 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency2<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '003' as postingPeriod,
          concat(fiscalYear,'003') as fiscalYearPeriod,
          totalValueTransactionCurrency3 as amountInTransactionCurrency,
          TotalValueControllingArea3 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency3<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '004' as postingPeriod,
          concat(fiscalYear,'004') as fiscalYearPeriod,
          totalValueTransactionCurrency4 as amountInTransactionCurrency,
          TotalValueControllingArea4 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency4<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '005' as postingPeriod,
          concat(fiscalYear,'005') as fiscalYearPeriod,
          totalValueTransactionCurrency5 as amountInTransactionCurrency,
          TotalValueControllingArea5 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency5<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '006' as postingPeriod,
          concat(fiscalYear,'006') as fiscalYearPeriod,
          totalValueTransactionCurrency6 as amountInTransactionCurrency,
          TotalValueControllingArea6 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency6<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '007' as postingPeriod,
          concat(fiscalYear,'007') as fiscalYearPeriod,
          totalValueTransactionCurrency7 as amountInTransactionCurrency,
          TotalValueControllingArea7 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency7<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '008' as postingPeriod,
          concat(fiscalYear,'008') as fiscalYearPeriod,
          totalValueTransactionCurrency8 as amountInTransactionCurrency,
          TotalValueControllingArea8 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency8<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '009' as postingPeriod,
          concat(fiscalYear,'009') as fiscalYearPeriod,
          totalValueTransactionCurrency9 as amountInTransactionCurrency,
          TotalValueControllingArea9 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency9<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '010' as postingPeriod,
          concat(fiscalYear,'010') as fiscalYearPeriod,
          totalValueTransactionCurrency10 as amountInTransactionCurrency,
          TotalValueControllingArea10 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency10<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '011' as postingPeriod,
          concat(fiscalYear,'011') as fiscalYearPeriod,
          totalValueTransactionCurrency11 as amountInTransactionCurrency,
          TotalValueControllingArea11 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency11<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '012' as postingPeriod,
          concat(fiscalYear,'012') as fiscalYearPeriod,
          totalValueTransactionCurrency12 as amountInTransactionCurrency,
          TotalValueControllingArea12 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency12<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '013' as postingPeriod,
          concat(fiscalYear,'013') as fiscalYearPeriod,
          totalValueTransactionCurrency13 as amountInTransactionCurrency,
          TotalValueControllingArea13 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency13<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '014' as postingPeriod,
          concat(fiscalYear,'014') as fiscalYearPeriod,
          totalValueTransactionCurrency14 as amountInTransactionCurrency,
          TotalValueControllingArea14 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency14<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '015' as postingPeriod,
          concat(fiscalYear,'015') as fiscalYearPeriod,
          totalValueTransactionCurrency15 as amountInTransactionCurrency,
          TotalValueControllingArea15 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency15<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          tradingPartnerID,
          tradingPartnerBusinessArea,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '016' as postingPeriod,
          concat(fiscalYear,'016') as fiscalYearPeriod,
          totalValueTransactionCurrency16 as amountInTransactionCurrency,
          TotalValueControllingArea16 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.cosp
          where totalValueTransactionCurrency16<>0

  )
  select * from cospInter where valueType='01' or valueType='41'
  UNION 
  select * from cospInter where valueType='04' or valueType='21' OR valueType='22' or valueType='45' and fiscalYear>2021
  UNION
  select * from cospInter where valueType='11' and fiscalYear<2022  
)
""")

# COMMAND ----------

# DBTITLE 1,Transaction Data - COSS
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}cleansed.fin.eppmCossTranspose
AS
(
  WITH cossInter AS
  (select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '001' as postingPeriod,
          concat(fiscalYear,'001') as fiscalYearPeriod,
          totalValueTransactionCurrency1 as amountInTransactionCurrency,
          TotalValueControllingArea1 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency1<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '002' as postingPeriod,
          concat(fiscalYear,'002') as fiscalYearPeriod,
          totalValueTransactionCurrency2 as amountInTransactionCurrency,
          TotalValueControllingArea2 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency2<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
        
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '003' as postingPeriod,
          concat(fiscalYear,'003') as fiscalYearPeriod,
          totalValueTransactionCurrency3 as amountInTransactionCurrency,
          TotalValueControllingArea3 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency3<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '004' as postingPeriod,
          concat(fiscalYear,'004') as fiscalYearPeriod,
          totalValueTransactionCurrency4 as amountInTransactionCurrency,
          TotalValueControllingArea4 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency4<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '005' as postingPeriod,
          concat(fiscalYear,'005') as fiscalYearPeriod,
          totalValueTransactionCurrency5 as amountInTransactionCurrency,
          TotalValueControllingArea5 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency5<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '006' as postingPeriod,
          concat(fiscalYear,'006') as fiscalYearPeriod,
          totalValueTransactionCurrency6 as amountInTransactionCurrency,
          TotalValueControllingArea6 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency6<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '007' as postingPeriod,
          concat(fiscalYear,'007') as fiscalYearPeriod,
          totalValueTransactionCurrency7 as amountInTransactionCurrency,
          TotalValueControllingArea7 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency7<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '008' as postingPeriod,
          concat(fiscalYear,'008') as fiscalYearPeriod,
          totalValueTransactionCurrency8 as amountInTransactionCurrency,
          TotalValueControllingArea8 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency8<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '009' as postingPeriod,
          concat(fiscalYear,'009') as fiscalYearPeriod,
          totalValueTransactionCurrency9 as amountInTransactionCurrency,
          TotalValueControllingArea9 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency9<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '010' as postingPeriod,
          concat(fiscalYear,'010') as fiscalYearPeriod,
          totalValueTransactionCurrency10 as amountInTransactionCurrency,
          TotalValueControllingArea10 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency10<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '011' as postingPeriod,
          concat(fiscalYear,'011') as fiscalYearPeriod,
          totalValueTransactionCurrency11 as amountInTransactionCurrency,
          TotalValueControllingArea11 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency11<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '012' as postingPeriod,
          concat(fiscalYear,'012') as fiscalYearPeriod,
          totalValueTransactionCurrency12 as amountInTransactionCurrency,
          TotalValueControllingArea12 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency12<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '013' as postingPeriod,
          concat(fiscalYear,'013') as fiscalYearPeriod,
          totalValueTransactionCurrency13 as amountInTransactionCurrency,
          TotalValueControllingArea13 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency13<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '014' as postingPeriod,
          concat(fiscalYear,'014') as fiscalYearPeriod,
          totalValueTransactionCurrency14 as amountInTransactionCurrency,
          TotalValueControllingArea14 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency14<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '015' as postingPeriod,
          concat(fiscalYear,'015') as fiscalYearPeriod,
          totalValueTransactionCurrency15 as amountInTransactionCurrency,
          TotalValueControllingArea15 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency15<>0

          UNION

          select ledgerForControllingObjects,
          objectnumber,
          fiscalYear,
          valueType,
          version,
          costElement,
          coKeySubNumber,
          cobusinessTransaction,
          partnerObject,
          sourceObject,
          debitCreditIndicator,
          transactionCurrency,
          periodBlock,
          '016' as postingPeriod,
          concat(fiscalYear,'016') as fiscalYearPeriod,
          totalValueTransactionCurrency16 as amountInTransactionCurrency,
          TotalValueControllingArea16 as amountInControllingAreaCurrency,
          debitType,
          companyCode,
          functionalArea 
          from {get_env()}cleansed.fin.coss
          where totalValueTransactionCurrency16<>0

  )
  select * from cossInter where valueType='01' or valueType='41'
  UNION 
  select * from cossInter where valueType='04' or valueType='21' OR valueType='22' or valueType='45' and fiscalYear>2021
  UNION
  select * from cossInter where valueType='11' and fiscalYear<2022
)
""")


# COMMAND ----------

# DBTITLE 1,Transaction Data - RPSCO
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}cleansed.ppm.eppmRpscoTranspose
AS
(
WITH rpscoInter AS      
  ( select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '000' as postingPeriod,
          concat(fiscalYear,'000') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue0 as amountInTransactionCurrency,
          periodValue0 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue0 <> 0 and fiscalYear <> 0

          UNION
  
    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '001' as postingPeriod,
          concat(fiscalYear,'001') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue01 as amountInTransactionCurrency,
          periodValue01 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue01 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '002' as postingPeriod,
          concat(fiscalYear,'002') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue02 as amountInTransactionCurrency,
          periodValue02 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue02 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '003' as postingPeriod,
          concat(fiscalYear,'003') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue03 as amountInTransactionCurrency,
          periodValue03 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue03 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '004' as postingPeriod,
          concat(fiscalYear,'004') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue04 as amountInTransactionCurrency,
          periodValue04 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue04 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '005' as postingPeriod,
          concat(fiscalYear,'005') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue05 as amountInTransactionCurrency,
          periodValue05 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue05 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '006' as postingPeriod,
          concat(fiscalYear,'006') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue06 as amountInTransactionCurrency,
          periodValue06 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue06 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '007' as postingPeriod,
          concat(fiscalYear,'007') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue07 as amountInTransactionCurrency,
          periodValue07 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue07 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '008' as postingPeriod,
          concat(fiscalYear,'008') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue08 as amountInTransactionCurrency,
          periodValue08 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue08 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '009' as postingPeriod,
          concat(fiscalYear,'009') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue09 as amountInTransactionCurrency,
          periodValue09 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue09 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '010' as postingPeriod,
          concat(fiscalYear,'010') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue10 as amountInTransactionCurrency,
          periodValue10 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue10 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '011' as postingPeriod,
          concat(fiscalYear,'011') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue11 as amountInTransactionCurrency,
          periodValue11 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue11 <> 0

          UNION

    select objectNumber,
          budgetLedger,
          valueType,
          objectIndicator,
          fiscalYear,
          valueCategory,
          budgetPlanning,
          planningVersion,
          analysisCategory,
          fund,
          '012' as postingPeriod,
          concat(fiscalYear,'012') as fiscalYearPeriod,
          transactionCurrency,
          periodBlock,
          debitType,
          periodValue12 as amountInTransactionCurrency,
          periodValue12 as amountInControllingAreaCurrency        
          from {get_env()}cleansed.ppm.rpsco
          where periodValue12 <> 0          
  )
select * from rpscoInter where valueType='01' or valueType='41'
UNION 
select * from rpscoInter where valueType='04' or valueType='21' OR valueType='22' or valueType='45' and fiscalYear>2021
UNION
select * from rpscoInter where valueType='11' and fiscalYear<2022
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER VIEW ppd_cleansed.fin.eppmCospTranspose OWNER TO `ppd-G3-Admins`;
# MAGIC -- ALTER VIEW ppd_cleansed.fin.eppmCossTranspose OWNER TO `ppd-G3-Admins`;
# MAGIC -- ALTER VIEW ppd_cleansed.ppm.eppmRpscoTranspose OWNER TO `ppd-G3-Admins`;
# MAGIC -- ALTER VIEW ppd_cleansed.ppm.eppmWpiConfig OWNER TO `ppd-G3-Admins`;
