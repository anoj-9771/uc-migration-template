# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{SOURCE}', 'aurion_employee_details')} AS
(
select distinct(*) from (
Select 'history' as Aurionfilename, employeeNumber , concat('HR8',RIGHT(concat('00000', personNumber) ,7)) as businessPartnerNumber, personNumber, UserID, dateCommenced, givenNames, surname, EmployeeStatus,
DateEffective,
ifnull(DateTo, now()) as DateTo,
datediff(ifnull(DateTo, now()),DateEffective) as DaysEmps,
PositionNumber, ReportstoPosition, OrganisationUnitNumber
from {get_table_namespace(f'{SOURCE}', 'aurion_employee_history')}
)
union
(
Select 'active' as Aurionfilename, employeeNumber , concat('HR8',RIGHT(concat('000000', personNumber),7)) as businessPartnerNumber, personNumber,
userId,dateCommenced, E.givenNames, E.surname, employeeStatus , dateEffective, ifnull(DateTo, now()) as DateTo , datediff(ifnull(DateTo, now()),DateEffective)
as DaysEmps, E.PositionNumber, ReportstoPosition , OrganisationUnitNumber
from {get_table_namespace(f'{SOURCE}', 'aurion_active_employees')} E
LEFT JOIN {get_table_namespace(f'{SOURCE}', 'aurion_position')} P on E.PositionNumber = P.PositionNumber
)
union
(
Select 'terminated' as Aurionfilename, employeeNumber , concat('HR8',RIGHT(concat('000000', personNumber) ,7)) as businessPartnerNumber, personNumber,
userId,dateCommenced, E.givenNames, E.surname,employeeStatus , dateEffective, ifnull(DateTo, now()) as DateTo ,datediff(ifnull(DateTo, now()),DateEffective) as
DaysEmps, E.PositionNumber, ReportstoPosition , OrganisationUnitNumber
from {get_table_namespace(f'{SOURCE}', 'aurion_terminated_employees')} E
LEFT JOIN {get_table_namespace(f'{SOURCE}', 'aurion_position')} P on E.PositionNumber = P.PositionNumber
))
""")

