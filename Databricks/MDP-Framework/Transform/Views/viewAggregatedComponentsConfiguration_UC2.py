# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated'
db = DEFAULT_TARGET

# COMMAND ----------

def CreateView(db: str, view: str, content: str, transform: dict):
    content_list = [line.split(',') for line in content.split('\n') if line]
    content_header = content_list[0]
    content_data = content_list[1:]

    if db:
        table_namespace = get_table_namespace(db, view)
        schema_name = '.'.join(table_namespace.split('.')[:-1])
        object_name = table_namespace.split('.')[-1]        
    elif len(view.split('.')) == 3:
        layer,schema_name,object_name = view.split('.')
        table_namespace = get_table_name(layer,schema_name,object_name)

    if len(table_namespace.split('.')) > 2:
        catalog_name = table_namespace.split('.')[0]
        spark.sql(f"USE CATALOG {catalog_name}")
    
    if spark.sql(f"SHOW VIEWS FROM {schema_name} LIKE '{object_name}'").count() == 1:
        sqlLines = f"ALTER VIEW {table_namespace} AS"
    else:
        sqlLines = f"CREATE VIEW {table_namespace} AS"

    sqlLines += "\nSELECT "+", ".join(transform.get(c,c) for c in content_header)+" FROM VALUES\n"
    sqlLines += ",\n".join("('"+"','".join(c for c in r)+"')" for r in content_data)
    sqlLines += "\nAS ("+", ".join(c for c in content_header)+")"
    print(sqlLines)
    spark.sql(sqlLines)    

# COMMAND ----------

view = "curated.water_balance.UnmeteredSTPPressureZoneLookup"
transform = {}
content = """
unmeteredSTP,uan,system,level,pressureZone,pressureZoneDescription
BONDI,ST0001,Water,60,P_DOVER_HEIGHTS,Dover Heights Pressure Zone
MALABAR,ST0002,Water,60,P_MAROUBRA,Maroubra Pressure Zone
FAIRFIELD,ST0008,Water,60,P_PROSPECT_P/L_GRAV,Prospect Pipeline Gravity Pressure Zone
LIVERPOOL,ST0009,Water,60,P_MT_PRITCHARD,Mt Pritchard Pressure Zone
CRONULLA,ST0010,Water,60,P_SUTHERLAND_RED_4,Sutherland Reduced 4 Pressure Zone
ST MARYS,ST0011,Water,60,P_ERSKINE_PARK,Erskine Park Pressure Zone
RICHMOND,ST0012,Water,60,P_NORTH_RICHMOND,North Richmond Pressure Zone
WOLLONGONG,ST0014,Water,60,P_MANGERTON_REM,Mangerton Remainder Pressure Zone
PORT KEMBLA,ST0015,Water,60,P_BERKELEY_RED_7,Berkeley Reduced 7 Pressure Zone
BELLAMBI,ST0016,Water,60,P_CORRIMAL_RED_1,Corrimal Reduced 1 Pressure Zone
QUAKERS HILL,ST0018,Water,60,P_MARAYONG_REM,Marayong Remainder Pressure Zone
NORTH HEAD,ST0020,Water,60,P_BEACON_HILL_BOOST_1,Beacon Hill Boost 1 Pressure Zone
WEST HORNSBY,ST0021,Water,60,P_WAHROONGA_RED_4,Wahroonga Reduced 4 Pressure Zone
BOMBO,ST0022,Water,60,P_KIAMA_REM,Kiama Remainder Pressure Zone
GLENFIELD,ST0023,Water,60,P_INGLEBURN_REM,Ingleburn Remainder Pressure Zone
CASTLE HILL,ST0024,Water,60,P_ROGANS_HILL_DMA_1,Rogans Hill DMA 1 Pressure Zone
WARRIEWOOD,ST0026,Water,60,P_NEWPORT_REM,Newport Remainder Pressure Zone
SHELLHARBOUR,ST0027,Water,60,P_OAK_FLATS_RED_1,Oak Flats Reduced 1 Pressure Zone
WEST CAMDEN,ST0028,Water,60,P_NARELLAN_SOUTH_REM,Narellan South Remainder Pressure Zone
ROUSE HILL,ST0031,Water,60,P_PARKLEA,Parklea Pressure Zone
NORTH RICHMOND,ST0033,Water,60,P_NORTH_RICHMOND,North Richmond Pressure Zone
HORNSBY HEIGHTS,ST0040,Water,60,P_HORNSBY_HEIGHTS_EL,Hornsby Heights Elevated Pressure Zone
RIVERSTONE,ST0042,Water,60,P_ROUSE_HILL,Rouse Hill Pressure Zone
PENRITH,ST0046,Water,60,P_PENRITH_NORTH,Penrith North Pressure Zone
WINMALEE,ST0064,Water,60,P_SPRINGWOOD_RED_2,Springwood Red 2 Pressure Zone
PICTON,ST0072,Water,60,P_THIRLMERE_RED_4,Thirlmere Reduced 4 Pressure Zone
WALLACIA,ST0073,Water,60,P_SILVERDALE_REM,Silverdale Remainder Pressure Zone
BROOKLYN,ST0074,Water,60,P_COWAN_NORTH_REM,Cowan North Remainder Pressure Zone
"""

CreateView(None, view, content, transform)

# COMMAND ----------

dfcolumns = spark.sql(f"""show columns from {get_env()}cleansed.sharepointlistedp.aggregatedcomponents""")
unpivotcount = 0
unpivotcolumns = ''
excludecolumns = ["ContentTypeID","ColorTag","ComplianceAssetId","Year","Month","Id","ContentType","Modified","Created","CreatedById","ModifiedById","Owshiddenversion","Version","Path"]
for i in dfcolumns.collect():
    if i.col_name not in excludecolumns and \
        not i.col_name.startswith("_"):
        unpivotcount +=1
        unpivotcolumns += f"'{i.col_name}',cast({i.col_name} as double),"
unpivotcolumns = unpivotcolumns[:-1]
# print(unpivotcolumns)        

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {get_env()}curated.water_balance.AggregatedComponentsConfiguration as
               with Base as 
               (Select Year as yearNumber, Month as monthName, stack({unpivotcount},{unpivotcolumns}) as (metricType,metricValueNumber),split(metricType,'_')[0] as metricTypeCategory,case when split(metricType,'_')[1] is not null then split(metricType,'_')[1] else NULL end lookupZoneName               
               FROM  {get_env()}cleansed.sharepointlistedp.aggregatedcomponents)
               select yearNumber,monthName,case when unmeteredSTP is not null then pressureZone
                                                when metricTypeCategory like '%SWOperationalUse%' then lookupZoneName
                                                when lookupZoneName is null then 'SydneyWater' end zoneName,
                                           case when unmeteredSTP is not null then 'PressureZone'
                                                when metricTypeCategory like '%SWOperationalUse%' then 'DeliverySystem'
                                                when lookupZoneName is null then 'SydneyWaterLevel' end zoneTypeName,metricTypeCategory as metricTypeName,metricValueNumber
               from Base left outer join {get_env()}curated.water_balance.UnmeteredSTPPressureZoneLookup on lower(lookupZoneName) = lower(replace(unmeteredSTP," ","")) 
               order by yearNumber desc""")
