# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

#PRADA-1181, PRADA-1412, PRADA-1607, PRADA-1718, PRADA-1719
def NatSpecialCharacterReplace(column):
  NATReplaceList = [["[\r\n\t\"“”®¿¬°º�­]", ""]
       ,["[\x7f\xa0]", ""]
       ,["[ªæ]", ""]
       ,["[·]", " "]
       ,["[‘’]", "'"]
       ,["[–]", "-"]
       ,["[ÅÁÃÀÀ]", "A"]
       ,["[Ç]", "C"]
       ,["[ÈÉ]", "E"] 
       ,["[ÌÍ]", "I"] 
       ,["[ÓÔÖØ]", "O"] 
       ,["[Ù]", "U"] 
       ,["[àáâãäå]", "a"]
       ,["[ß]", "B"]
       ,["[ç]", "c"]
       ,["[èéëêê]", "e"]
       ,["[ñ]", "n"]
       ,["[ìíï]", "i"]
       ,["[óõöøòô]", "o"]
       ,["[úüµ]", "u"]
       ,["[ÿÿ]", "y"]
       ,["[ž]", "z"]
]
  return GeneralSpecialCharacterReplace(column, NATReplaceList)

# COMMAND ----------

def NatValidCharacterReplace(column):
  NATReplaceList = [["[1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM@$,=^%-/\\\\><\~\[\]}}{{||;+()*_&'#`?!:\'\" ]", ""]]
  return GeneralSpecialCharacterReplace(column, NATReplaceList)

# COMMAND ----------

def GetBadCharacters(dataFrame : object, asArray = False):
  badCharArray = sorted(set("".join(c.S for c in dataFrame.withColumn("Output", NatValidCharacterReplace(NatSpecialCharacterReplace(concat(*[coalesce(c, lit("")) for c in dataFrame.columns])))).selectExpr("Output S").distinct().collect())))
  
  return badCharArray if asArray else "".join(badCharArray)

# COMMAND ----------

def GlobalSpecialCharacterReplacement(dataFrame : object, replacementChar = "?"):
  df = dataFrame
  badChars = GetBadCharacters(df)
  count = len(badChars)
  
  if count > 0: 
    LogEtl(f"*** Warning *** {count} new bad characters found: [{badChars}]! Replacement with [{replacementChar}] applied.")

    for c in df.columns:
      df = df.withColumn(c, GeneralSpecialCharacterReplace(c, [[f"[{badChars}]", replacementChar]]))
  
  return df

# COMMAND ----------

