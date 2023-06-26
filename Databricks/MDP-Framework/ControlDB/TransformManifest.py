# Databricks notebook source
# MAGIC %run ./common-controldb

# COMMAND ----------

def CleanTransformConfig():
    print(f"Cleaning from TransformManifest...")
    ExecuteStatement(f"""
        TRUNCATE TABLE dbo.transformmanifest
    """)

# COMMAND ----------

def AddTransform(df, clean = True):
    if clean:
        CleanTransformConfig()
        
    print(f"Inserting into TransformManifest...")    
    for i in df.collect():
        ExecuteStatement(f"""    
        INSERT INTO dbo.transformmanifest (
            TransformID
            ,EntityType
            ,EntityName
            ,ProcessorType
            ,TargetKeyVaultSecret
            ,Command
            ,Dependancies
            ,ParallelGroup
            ,Enabled
            ,CreatedDTS
        ) VALUES(
            {i.TransformID}
            ,NULLIF('{i.EntityType}','')
            ,NULLIF('{i.EntityName}','')
            ,NULLIF('{i.ProcessorType}','')
            ,'{i.TargetKeyVaultSecret}'
            ,NULLIF('{i.Command}','')
            ,NULLIF('{i.ExternalDependency}','')
            ,{i.ParallelGroup}
            ,{i.Enabled}
            ,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time')
        )
        """)   

# COMMAND ----------

def ShowTransformConfig():
    print(f"Showing TransformManifest...")
    display(
        spark.table("controldb.dbo_transformmanifest")
    )

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr, col
transform_df = spark.sql("""
    SELECT * FROM VALUES
    (1,'Fact','AssetDemandValue','databricks-notebook','',null,'iicats',1),
    (2,'Fact','Demand','databricks-notebook','',1,null,1),
    (3,'Dim','AssetContract','databricks-notebook','',null,'maximo',1),
    (4,'Dim','AssetLocation','databricks-notebook','',null,'maximo',1),
    (5,'Dim','WorkOrderJobPlan','databricks-notebook','',null,'maximo',1),
    (6,'Dim','WorkOrderProblemType','databricks-notebook','',null,'maximo',1),
    (7,'Dim','Asset','databricks-notebook','',4,null,1),
    (8,'Dim','AssetLocationAncestor','databricks-notebook','',4,null,1),
    (9,'Dim','LocationSpec','databricks-notebook','',4,null,1),
    (10,'Dim','AssetMeter','databricks-notebook','',7,null,1),
    (11,'Dim','AssetSpec','databricks-notebook','',7,null,1),
    (12,'Fact','WorkOrder','databricks-notebook','',7,null,1),
    (13,'Fact','PreventiveMaintenance','databricks-notebook','',6,null,1),
    (14,'Fact','WorkOrderFailureReport','databricks-notebook','',12,null,1),
    (15,'Fact','AssetPerformance','databricks-notebook','',12,null,1),
    (16,'Fact','AssetPerformanceIndex','databricks-notebook','',12,null,1),
    (17,'Bridge','BridgeWorkOrderPreventiveMaintenance','databricks-notebook','',12,null,1)
    AS (TransformID,EntityType,EntityName,ProcessorType,TargetKeyVaultSecret,InternalDependency,ExternalDependency,Enabled)
""")

level = 1
RECURSION_LIMIT=25
df = (
      transform_df
      .where(col('InternalDependency').isNull())
      .withColumn('ParallelGroup', lit(level))
)

recursion_df = df
while True:
    # select data for this recursion level
    level_df = (
        recursion_df.alias('r').join(transform_df.alias('t'), expr('r.TransformID == t.InternalDependency'))
        .selectExpr('t.*','r.ParallelGroup + 1 as ParallelGroup')
    )        
    # display(level_df)
    recursion_df = level_df
    df = df.union(level_df)
    if level_df.count() == 0:
        break
    else:
        level+=1
        if level == RECURSION_LIMIT:
            raise Exception(f'RECURSION_LIMIT {RECURSION_LIMIT} reached. Check that transforms do not have cyclical dependency.')

df = df.selectExpr(
    'TransformID'
    ,'EntityType'
    ,'EntityName'
    ,'ProcessorType'
    ,'TargetKeyVaultSecret'
    ,"""'/MDP-Framework/Transform/'
        ||if(EntityType='Dim','Dimension',EntityType)
        ||'/'||EntityName Command"""
    ,'InternalDependency'
    ,'ExternalDependency'
    ,'ParallelGroup'
    ,'Enabled'
)

df.display()

# COMMAND ----------

def ConfigureTransformManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    display(df)

    # ------------- SAVE ----------------- #
    AddTransform(df, clean=True)
    
    # ------------- ShowConfig ----------------- #
    ShowTransformConfig()
ConfigureTransformManifest(df)    