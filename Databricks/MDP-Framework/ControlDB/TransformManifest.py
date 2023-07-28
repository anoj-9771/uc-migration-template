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
    (13,'Fact','PreventiveMaintenance','databricks-notebook','',7,null,1),
    (14,'Fact','WorkOrderFailureReport','databricks-notebook','',12,null,1),
    (15,'Fact','AssetPerformance','databricks-notebook','',12,null,1),
    (16,'Fact','AssetPerformanceIndex','databricks-notebook','',12,null,1),
    (17,'Bridge','WorkOrderPreventiveMaintenance','databricks-notebook','',12,null,1),
    (18,'Dim','BusinessPartner','databricks-notebook','',null,null,0),
    (19,'Dim','CommunicationChannel','databricks-notebook','',null,null,0),
    (20,'Dim','CustomerInteractionStatus','databricks-notebook','',null,null,0),
    (21,'Dim','CustomerServiceAttachmentInfo','databricks-notebook','',null,null,0),
    (22,'Dim','CustomerServiceCategory','databricks-notebook','',null,null,0),
    (23,'Dim','CustomerServiceEmailHeader','databricks-notebook','',null,null,0),
    (24,'Dim','CustomerServiceProcessType','databricks-notebook','',null,null,0),
    (25,'Dim','CustomerServiceRequestStatus','databricks-notebook','',null,null,0),
    (26,'Dim','Survey','databricks-notebook','',null,null,0),
    (27,'Dim','SurveyParticipant','databricks-notebook','',26,null,0),
    (28,'Dim','SurveyQuestion','databricks-notebook','',26,null,0),
    (29,'Dim','SurveyResponseInformation','databricks-notebook','',26,null,0),
    (30,'Fact','CustomerInteraction','databricks-notebook','',29,null,0),
    (31,'Fact','CustomerServiceEmailEvent','databricks-notebook','',29,null,0),
    (32,'Fact','CustomerServiceRequest','databricks-notebook','',29,null,0),
    (33,'Fact','CustomerServiceWorkNote','databricks-notebook','',29,null,0),
    (34,'Fact','SurveyMiscellaneousInformation','databricks-notebook','',29,null,0),
    (35,'Fact','SurveyResponse','databricks-notebook','',29,null,0),
    (36,'Bridge','CustomerInteractionAttachment','databricks-notebook','',35,null,0),
    (37,'Bridge','CustomerInteractionEmail','databricks-notebook','',35,null,0),
    (38,'Bridge','CustomerInteractionServiceRequest','databricks-notebook','',35,null,0),
    (39,'Bridge','CustomerInteractionWorkNoteSummary','databricks-notebook','',35,null,0),
    (40,'Bridge','CustomerServiceRequestAttachment','databricks-notebook','',35,null,0),
    (41,'Bridge','CustomerServiceRequestEmail','databricks-notebook','',35,	null,0),
    (42,'Bridge','CustomerServiceRequestInteraction','databricks-notebook','',35,null,0),
    (43,'Bridge','CustomerServiceRequestSurvey','databricks-notebook','',35,null,0),
    (44,'Bridge','CustomerServicetoServiceRequest','databricks-notebook','',35,null,0),
    (45,'Bridge','CustomerServiceWorkResolution','databricks-notebook','',35,null,0),
    (46,'Bridge','CustomerServiceWorkNoteSummary','databricks-notebook','',35,null,0),
    (47,'Bridge','CustomerInteractionSurvey','databricks-notebook','',35,null,0),
    (48,'Fact','UnmeteredConsumption','databricks-notebook','',null,null,1),
    (49,'Fact','ConsumptionAggregate','databricks-notebook','',48,null,1),
    (50,'Fact','StoppedMeterConsumption','databricks-notebook','',null,null,1),
    (51,'Fact','StoppedMeterAggregate','databricks-notebook','',50,null,1),
    (52,'Views','viewUnmeteredConsumption_UC2','databricks-notebook','',49,null,1)
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
