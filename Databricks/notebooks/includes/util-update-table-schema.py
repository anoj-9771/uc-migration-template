# Databricks notebook source
#Function: verifyTableSchema(table as string, new structure definition as StructType)<br />
#<p>
#This function will allow you to verify that the structure definition in the code matches the table definition. 
#If it doesn't it will generate and execute ALTER statements to update the table schema

# COMMAND ----------

print('verifyTableSchema(table as string, new structure definition as StructType)')
print('\tThis function will verify nullable flags in schema against table definition and generate/run ALTER statements as required')

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
    
    #assert len(currentStruct) == len(desiredStruct), f'table contains {len(currentStruct)} columns and schema contains {len(desiredStruct)} columns'
    
    for ix, elem in enumerate(currentStruct):
        #make sure we are looking at the same element in both places
        #assert elem[0] == desiredStruct[ix][0], f'Column {elem[0]} on table does not match {desiredStruct[ix][0]} in schema definition'
        #should we also change data types here? It seems to work from standard code anywayand would need a conversion from Python to SQL type for it to work here
        #if elem[1] != desiredStruct[ix][1]:
        #    alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} TYPE {desiredStruct[ix][1]};')
        #if the nullable flag is not the same on both sides, generate an ALTER statement to update the table
        try:
            # if element matches
            assert elem[0] == desiredStruct[ix][0], f'Column {elem[0]} on table does not match {desiredStruct[ix][0]} in schema definition'
            
            if elem[2] != desiredStruct[ix][2]:
                alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} {"SET NOT NULL" if desiredStruct[ix][2] == "false" else "DROP NOT NULL"};')
        except IndexError:
            if elem[0][:1] == '_': #system field
                # must be False
                if elem[2] != 'false':
                    alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} SET NOT NULL;')
            else:
                raise
                
    #execute the ALTER statements
    for alterStmt in alterStmts:
        print(alterStmt)
        spark.sql(alterStmt)
