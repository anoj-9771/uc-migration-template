# Databricks notebook source
# MAGIC %md
# MAGIC Function: verifyTableSchema(table as string, new structure definition as StructType)<br />
# MAGIC <p>
# MAGIC This function will allow you to verify that the structure definition in the code matches the table definition. 
# MAGIC If it doesn't it will generate and execute ALTER statements to update the table schema

# COMMAND ----------

def verifyTableSchema(table, newSchema):
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, FloatType, DecimalType, DateType, LongType
    
    dfStruct = []
    desiredStruct = []
    alterStmts = []
    
    #build list of struture type elements
    def buildStruct(schema):
        struct = []
        for ix, fld in enumerate(str(schema).split('StructField')):
            if ix == 0:
                continue
            flds = fld.strip('(').strip('),').split(',')
            if flds[1][:11] == 'DecimalType':
                flds[1] += ',' + flds[2]
                flds[2] = flds[3]
            struct.append([flds[0], flds[1], flds[2]])
        return(struct)

    df = spark.table(table)
    currentStruct = buildStruct(df.schema)
    desiredStruct = buildStruct(newSchema)
    
    assert len(currentStruct) == len(desiredStruct)
    
    for ix, elem in enumerate(currentStruct):
        #make sure we are looking at the same element in both places
        assert elem[0] == desiredStruct[ix][0]
        #should we also change data types here? It seems to work from standard code anywayand would need a conversion from Python to SQL type for it to work here
        #if elem[1] != desiredStruct[ix][1]:
        #    alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} TYPE {desiredStruct[ix][1]};')
        #if the nullable flag is not the same on both sides, generate an ALTER statement to update the table
        if elem[2] != desiredStruct[ix][2]:
            alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} {"SET NOT NULL" if desiredStruct[ix][2] == "false" else "DROP NOT NULL"};')
            
    #execute the ALTER statements
    for alterStmt in alterStmts:
        print(alterStmt)
        spark.sql(alterStmt)
