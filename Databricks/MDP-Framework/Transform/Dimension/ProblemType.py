# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_swcProblemType").select("problemTypeId","problemType","description")
   
    # ------------- JOINS ------------------ #
    
    
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"problemType {BK}"
        ,"problemTypeId problemTypeIdSK"
        ,"problemType problemType"
        ,"description problemDescription"
       
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


