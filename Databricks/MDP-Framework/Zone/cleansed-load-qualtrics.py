# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.caseSensitive", "true")
task = dbutils.widgets.get("task")
j = json.loads(task)
systemCode = j.get("SystemCode")
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
cleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
sourceTableName = f"raw.{destinationSchema}_{destinationTableName}"

# COMMAND ----------

def RemoveDuplicateColumns(dataFrame):
    seen = set()
    dupes = [x for x in dataFrame.columns if x.lower() in seen or seen.add(x.lower())]
    for d in dupes:
        dataFrame = dataFrame.drop(d)
    return dataFrame

# COMMAND ----------

template = f"RIGHT(SHA2(CAST($c$ AS STRING), 256), 16)"
masks = {
    "redacted" : "'[*** REDACTED ***]'"
    ,"firstname" : template
    ,"lastname" : template
    ,"email" : template
    ,"address" : template
    ,"phone" : template
    ,"mobile" : template
    ,"mob" : template
    ,"datareference" : template
    ,"longitude" : template
    ,"applicant" : template
}

def MaskPIIColumn(column):
    for k, v in masks.items():
        if k in column.lower():
            return f"$l$ `{column}`".replace("$l$", v.replace("$c$", f"`{column}`"))
    return f"`{column}`"     

def MaskTable(dataFrame):
    return dataFrame.selectExpr(
        [MaskPIIColumn(c) for c in dataFrame.columns]
    )

# COMMAND ----------

import json, re
def EnrichResponses(dataFrame):
    global qid_dict
    global qid_dict2
    dfq = spark.table(f"cleansed.{destinationSchema}_{re.sub('Responses$','Questions',destinationTableName)}")
    questions = [row.asDict(True) for row in dfq.select('questionID','questionText','choices','answers','questionType','selector','subSelector').collect()]
    qid_dict = {}
    for question in questions:
       qid = question.pop('questionID')
       qid_dict[qid] = question
        
    qid_dict2 = {}
    for key,value in qid_dict.items():
        d={}
        for k in value:
            if k in ['answers','choices']:
                #Choices can be a string if they're not all dictionaries for all questions.
                if isinstance(value[k], str):
                    print(f"value[k] => {value[k]}")
                    d[k] = json.loads(value[k])
                else:
                    d[k] = value[k]
                #Some Choices are lists instead of dicts. 
                #Convert to dict to satisfy from_json's expectation of consistent data type.
                if isinstance(d[k], list):
                    d[k] = {str(i):item for i, item in enumerate(d[k])}
        qid_dict2[key] = d
        
    dataFrame = dataFrame.withColumn('qid_lookup', from_json(lit(json.dumps(qid_dict2)), 'map<string, map<string, map<string, map<string, string>>>>'))
    
    qid_columns = [column for column in dataFrame.schema.fieldNames() if column.startswith('QID')]    

    #Add column for each main question
    for qid in set(column.split('_')[0] for column in qid_columns):
        if qid_dict.get(qid, {}).get('questionType') not in ['Meta']:
            dataFrame = dataFrame.withColumn(f'question{re.sub("^QID","",qid)}QuestionText', lit(qid_dict.get(qid, {}).get('questionText')))
    
    rtext_dict={'TopicSenScore': 'TopicSentimentScore', 'ParTopics': 'ParTopicsText',
                'TopicSenLabel': 'TopicsSentimentsLabel','Sentiment': 'SentimentDsc',
                'SenPol': 'SentimentPolarityNumber', 'SenScore': 'SentimentScore',
                'Topics': 'TopicsText'}    
    for column in qid_columns:
        parts = column.split('_')
        qid = parts[0]
        prefix_col_name = f'question{re.sub("^QID","",qid)}'
        col_has_text = False
        if len(parts) > 1 and parts[1].isnumeric():
            qid_part = parts[1]
            prefix_col_name = f'{prefix_col_name}Part{qid_part}'    
            col_has_text = 'TEXT' in parts
            qtext_col_name = f'{prefix_col_name}QuestionText'

        rcode_col_name = f'{prefix_col_name}ResponseCode'
        
        for key,value in rtext_dict.items():
            if column.endswith(key):        
                rtext_col_name = f'{prefix_col_name}{value}'
                break
        else:        
            rtext_col_name = f'{prefix_col_name}ResponseText'   

        if qid_dict.get(qid, {}).get('questionType') == 'Meta':
            continue
        elif qid_dict.get(qid, {}).get('questionType') == 'TE' or col_has_text:
            dataFrame = dataFrame.withColumnRenamed(column, rtext_col_name)
        
        elif qid_dict.get(qid, {}).get('questionType') == 'DB' and qid_dict.get(qid, {}).get('selector') == 'TB':
            continue        
            
        elif qid_dict.get(qid, {}).get('questionType') == 'MC' and qid_dict.get(qid, {}).get('selector') == 'DL':
            dataFrame = dataFrame.withColumn(rtext_col_name, col('qid_lookup')[qid]['choices'][col(column)]['Display'])
            
        elif qid_dict.get(qid, {}).get('questionType') == 'MC' and qid_dict.get(qid, {}).get('selector') == 'SAVR':
            dataFrame = dataFrame.withColumn(rtext_col_name, col('qid_lookup')[qid]['choices'][col(column)]['Display'])

        elif qid_dict.get(qid, {}).get('questionType') == 'MC' and qid_dict.get(qid, {}).get('selector') == 'SAHR':
            dataFrame = dataFrame.withColumn(rtext_col_name, col('qid_lookup')[qid]['choices'][col(column)]['Display'])

        elif qid_dict.get(qid, {}).get('questionType') == 'MC' and qid_dict.get(qid, {}).get('selector') == 'MAVR' and not(col_has_text):
            dataFrame = dataFrame.withColumn(rtext_col_name, transform(col(column), lambda x: col('qid_lookup')[qid]['choices'][x]['Display']))

        elif qid_dict.get(qid, {}).get('questionType') == 'Matrix' and qid_dict.get(qid, {}).get('selector') == 'Likert':
            dataFrame = dataFrame.withColumn(qtext_col_name, col('qid_lookup')[qid]['choices'][qid_part]['Display'])
            dataFrame = dataFrame.withColumn(rtext_col_name, col('qid_lookup')[qid]['answers'][col(column)]['Display'])

        #Rename qid column to Response code
        if qid_dict.get(qid, {}).get('questionType') not in ['Meta','TE'] and not(col_has_text):
            dataFrame = dataFrame.withColumnRenamed(column, rcode_col_name)
            
    dataFrame = dataFrame.select(sorted(dataFrame.columns)).drop('qid_lookup')
    return dataFrame

# COMMAND ----------

sourceDataFrame = spark.table(sourceTableName)

# CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
if(extendedProperties):
  extendedProperties = json.loads(extendedProperties)
  cleansedQuery = extendedProperties.get("CleansedQuery")
  if(cleansedQuery):
    sourceDataFrame = spark.sql(cleansedQuery.replace("{tableFqn}", sourceTableName))
    
# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(RemoveBadCharacters(c) for c in sourceDataFrame.columns))

# REMOVE DUPE COLUMNS
sourceDataFrame = RemoveDuplicateColumns(sourceDataFrame)

# APPLY RULES-BASED CLEANSED FRAMEWORK
sourceDataFrame = CleansedTransformByRules(sourceDataFrame, sourceTableName, systemCode)

# Enrich responses with questions information
if destinationTableName.endswith('Responses'):
    sourceDataFrame = EnrichResponses(sourceDataFrame)

# MASK TABLE
sourceDataFrame = MaskTable(sourceDataFrame)

tableName = f"{destinationSchema}_{destinationTableName}"
CreateDeltaTable(sourceDataFrame, f"cleansed.{tableName}", dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(sourceDataFrame, f"cleansed.{tableName}", dataLakePath, j.get("BusinessKeyColumn"))
