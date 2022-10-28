# Databricks notebook source
def ExpandTable(df):
    newDf = df
    for i in df.dtypes:
        columnName = i[0]
        
        if i[1].startswith("struct"):
            newDf = newDf.selectExpr("*", f"`{columnName}`.*")
            newDf = newDf.drop(columnName)
            return ExpandTable(newDf)
        if i[1].startswith("array") and "struct" in i[1]:
            explodedDf = newDf.withColumn(f"{columnName}", expr(f"explode(`{columnName}`)"))
            newDf = explodedDf.selectExpr("*", f"`{columnName}`.*")

            for c in explodedDf.selectExpr(f"`{columnName}`.*").columns:
                newDf = newDf.withColumnRenamed(c, f"{columnName}_{c}".replace("__", "_"))
            newDf = newDf.drop(columnName)
            return ExpandTable(newDf)
    return newDf
