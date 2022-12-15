# Databricks notebook source
# MAGIC %md # This notebook needs cluster "riverwatch" for BN model library

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET JOB POOL TO USE UTC DATE. THIS IS NECESSARY OTHERWISE OLD DATA WOULD OTHERWISE BE RETRIEVED
# MAGIC SET TIME ZONE 'Australia/Sydney';

# COMMAND ----------

# MAGIC %md # Import libraries

# COMMAND ----------

# ----------------    Spark libraries---------------------------------
from pyspark.sql import functions as psf
from pyspark.sql import Window as W
from pyspark.sql import types as t

# ------------------ Below are libraries for BN model, need to run using cluster "riverwatch" -----------------

import pandas as pd  # for data manipulation
import numpy as np

# for creating Bayesian Belief Networks (BBN)
from pybbn.graph.dag import Bbn
from pybbn.graph.edge import Edge, EdgeType
from pybbn.graph.jointree import EvidenceBuilder
from pybbn.graph.node import BbnNode
from pybbn.graph.variable import Variable
from pybbn.pptc.inferencecontroller import InferenceController

# --------------------Libraries for registering the model in MLflow--------------------
import mlflow
import mlflow.pyfunc
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import time

# COMMAND ----------

# MAGIC %md # BN model traing data

# COMMAND ----------

# MAGIC %md ## Training data categorisation
# MAGIC 
# MAGIC The training data is a pandas dataframe, since the BN model library is in pandas type

# COMMAND ----------

### so it is the issue with the data format when read the data using spark or pd
#---------------------dicretise the data for each node---------------------------
# df_train=pd.read_csv("/dbfs/FileStore/DataLab/Riverwatch/df_train_bef2014.csv")
# df_train=pd.read_csv("/dbfs/FileStore/DataLab/Riverwatch/df_train_bef2019Jul.csv")
df_train=pd.read_csv("/dbfs/mnt/blob-urbanplunge/df_train_bef2019Jul.csv")
df_train['ent_cat'] = df_train['ent'].apply(
    lambda x: '0.0-32' if 0 <= x < 32 
    else '1.32-39' if 32 <= x < 39 
    else '2.39-51' if 39 <= x < 51 
    else '3.51-158' if 51 <= x < 158 
    else '4.>=158')
df_train['EC_cat'] = df_train['EC'].apply(
    lambda x: '0.0-40' if 0 <= x < 40 
    else '1.40-45' if 40 <= x < 45 
    else '2.45-50' if 45 <= x < 50 
    else '3.>=50')
df_train['solar_24_cat'] = df_train['solar_24'].apply(
    lambda x: '0.0-1' if 0 <= x < 1 
    else '1.1-4' if 1 <= x < 4 
    else '2.4-9' if 4 <= x < 9 
    else '3.>=9')
df_train['sun_24_cat'] = df_train['sun_24'].apply(
    lambda x: '0.0-1' if 0 <= x < 1 
    else '1.1-5' if 1 <= x < 5 
    else '2.>=5')
df_train['rain_24_cat'] = df_train['rain_24'].apply(
    lambda x: '0.0' if x == 0 
    else '1.0-6' if 0 < x < 6 
    else '2.6-12' if 6 <= x < 12 
    else '3.12-20' if 12 <= x < 20 
    else '4.>=20')
df_train['rain_48_cat'] = df_train['rain_48'].apply(
    lambda x: '0.0' if x == 0 
    else '1.0-6' if 0 < x < 6 
    else '2.6-14' if 6 <= x < 14 
    else '3.14-20' if 14 <= x < 20
    else '4.>=20')
df_train['rain_72_cat'] = df_train['rain_72'].apply(
    lambda x: '0.0' if x == 0 
    else '1.0-14' if 0 < x < 14 
    else '2.14-30' if 14 <= x < 30 
    else '3.>=30')
df_train['rain_7d_cat'] = df_train['rain_7d'].apply(
    lambda x: '0.0-10' if 0 <= x < 10 
    else '1.10-50' if 10 <= x < 50 
    else '2.>=50')
df_train['Rintensity_cat'] = df_train['Rintensity'].apply(
    lambda x: '0.0' if x == 0 
    else '1.0-2' if 0 < x < 2 
    else '2.2-4' if 2 <= x < 4 
    else '3.>=4')
df_train['Rduration_cat'] = df_train['Rduration'].apply(
    lambda x: '0.0' if x == 0 
    else '1.0-2' if 0 < x < 2 
    else '2.2-4' if 2 <= x < 4 
    else '3.>=4')
df_train['Rdistribution_cat'] = df_train['Rdistribution'].apply(
    lambda x: '0.0' if x == 0 
    else '1.0-5' if 0 < x < 7 
    else '2.>=7')
df_train['stormwater_pct_cat'] = df_train['stormwater_pct'].apply(
    lambda x: '0.0-5' if 0 <= x < 5 
    else '1.5-15' if 5 <= x < 15 
    else '2.15-25' if 15 <= x < 25 
    else '3.>=25')
df_train['days_after_rain_20mm_cat'] = df_train['days_after_rain_20mm'].apply(
    lambda x: '0.0-1' if 0 <= x < 1 
    else '1.1-2' if 1 <= x < 2 
    else '2.2-3' if 2 <= x < 3 
    else '3.3-4' if 3 <= x < 4 
    else '4.>=4')

# COMMAND ----------

# MAGIC %md # Build the BN model

# COMMAND ----------

# MAGIC %md ## CPT function

# COMMAND ----------

#functions for creating CPT
def ceildiv(a, b):
    return -(a // -b)

def flatten(xss):
    return [x for xs in xss for x in xs]

# This function helps to calculate probability distribution, which goes into BBN (note, can handle up to 3 parents)
def cpt(data, child, parent1=None, parent2=None, parent3=None, child_cat_number=None, chcat=None, pcat1=None,
        pcat2=None, pcat3=None):
    if parent1 == None:
        # Calculate probabilities
        # prob=pd.crosstab(data[child], 'Empty', margins=False, dropna=False).sort_index().to_numpy().reshape(-1).tolist()
        fooresults = pd.crosstab(pd.Categorical(data[child], categories=chcat), 'Empty', margins=False, dropna=False)
        probcountlist = np.reshape(fooresults.values, (1, int(child_cat_number))).tolist()[0]
        probcountlearninglist = [x + 1 for x in probcountlist]
        probclmatrix = np.reshape(probcountlearninglist,
                                  (ceildiv(len(probcountlist), int(child_cat_number)), int(child_cat_number)))
        rowsum = np.sum(probclmatrix, axis=1)
        probclmatrix_norm = probclmatrix / np.reshape(rowsum, (len(rowsum), 1))
        proboutlist = np.reshape(probclmatrix_norm, (1, len(probcountlist))).tolist()
        prob = proboutlist[0]
    elif parent1 != None:
        # Check if child node has 1 parent or 2 parents
        if parent2 == None:
            # Caclucate probabilities
            # prob=pd.crosstab(data[parent1],data[child], margins=False, dropna=False).sort_index().to_numpy().reshape(-1).tolist()
            p1 = pd.Categorical(data[parent1], categories=pcat1)
            cd = pd.Categorical(data[child], categories=chcat)
            probcountlisttmp = pd.crosstab(p1, cd, margins=False, dropna=False)
            probcountlist = flatten(probcountlisttmp.values)
            probcountlearninglist = [x + 1 for x in probcountlist]
            probclmatrix = np.reshape(probcountlearninglist,
                                      (len(probcountlist) // int(child_cat_number), int(child_cat_number)))
            rowsum = np.sum(probclmatrix, axis=1)
            probclmatrix_norm = probclmatrix / np.reshape(rowsum, (len(rowsum), 1))
            proboutlist = np.reshape(probclmatrix_norm, (1, len(probcountlist))).tolist()
            prob = proboutlist[0]
        elif parent2 != None:
            # Check if child node has 2 parent or 3 parents
            if parent3 == None:
                # Caclucate probabilities
                # prob=pd.crosstab([data[parent1],data[parent2]],data[child], margins=False, dropna=False).sort_index().to_numpy().reshape(-1)
                p1 = pd.Categorical(data[parent1], categories=pcat1)
                p2 = pd.Categorical(data[parent2], categories=pcat2)
                cd = pd.Categorical(data[child], categories=chcat)
                probcountlisttmp = pd.crosstab([p1, p2], cd, margins=False, dropna=False)
                probcountlist = flatten(probcountlisttmp.values)
                probcountlearninglist = [x + 1 for x in probcountlist]
                probclmatrix = np.reshape(probcountlearninglist,
                                          (len(probcountlist) // int(child_cat_number), int(child_cat_number)))
                rowsum = np.sum(probclmatrix, axis=1)
                probclmatrix_norm = probclmatrix / np.reshape(rowsum, (len(rowsum), 1))
                proboutlist = np.reshape(probclmatrix_norm, (1, len(probcountlist))).tolist()
                prob = proboutlist[0]
            else:
                # prob=pd.crosstab([data[parent1],data[parent2],data[parent3]], data[child], margins=False, dropna=False).sort_index().to_numpy().reshape(-1).tolist()
                p1 = pd.Categorical(data[parent1], categories=pcat1)
                p2 = pd.Categorical(data[parent2], categories=pcat2)
                p3 = pd.Categorical(data[parent3], categories=pcat3)
                cd = pd.Categorical(data[child], categories=chcat)
                probcountlisttmp = pd.crosstab([p1, p2, p3], cd, margins=False, dropna=False)
                probcountlist = flatten(probcountlisttmp.values)
                probcountlearninglist = [x + 1 for x in probcountlist]
                probclmatrix = np.reshape(probcountlearninglist,
                                          (len(probcountlist) // int(child_cat_number), int(child_cat_number)))
                rowsum = np.sum(probclmatrix, axis=1)
                probclmatrix_norm = probclmatrix / np.reshape(rowsum, (len(rowsum), 1))
                proboutlist = np.reshape(probclmatrix_norm, (1, len(probcountlist))).tolist()
                prob = proboutlist[0]
    else:
        print("Error in Probability Frequency Calculations")
    return prob

# COMMAND ----------

# MAGIC %md ## Creat node

# COMMAND ----------

# ----------------------pre-set input factors discretisation range-------------------
catranges_su24 = ['0.0-1', '1.1-5', '2.>=5']
catranges_rdur = ['0.0', '1.0-2', '2.2-4', '3.>=4']
catranges_ent = ['0.0-32', '1.32-39', '2.39-51', '3.51-158', '4.>=158']
catranges_r24 = ['0.0', '1.0-6', '2.6-12', '3.12-20', '4.>=20']
catranges_r48 = ['0.0', '1.0-6', '2.6-14', '3.14-20', '4.>=20']
catranges_r72 = ['0.0', '1.0-14', '2.14-30', '3.>=30']
catranges_r7d = ['0.0-10', '1.10-50', '2.>=50']
catranges_rdis = ['0.0', '1.0-5', '2.>=7']
catranges_so24 = ['0.0-1', '1.1-4', '2.4-9', '3.>=9']
catranges_ecc = ['0.0-40', '1.40-45', '2.45-50', '3.>=50']
catranges_rint = ['0.0', '1.0-2', '2.2-4', '3.>=4']
catranges_ctopct = ['0.0-5', '1.5-15', '2.15-25', '3.>=25']
catranges_dar20 = ['0.0-1', '1.1-2', '2.2-3', '3.3-4', '4.>=4']

# print(df_train.dtypes)
#---------------------prepare node information for creating node and cpt function---------------------------
var_su24 =   [0, 'su24', catranges_su24]
cpt_su24 =   ['sun_24_cat', 'ent_cat', 'solar_24_cat',None, '3', catranges_su24, catranges_ent, catranges_so24,None]
var_rdur =   [1, 'rdur', catranges_rdur]
cpt_rdur =   ['Rduration_cat', 'ent_cat', 'Rintensity_cat',None, '4', catranges_rdur, catranges_ent, catranges_rint,None]
var_ent =    [2, 'ent', catranges_ent]
cpt_ent =    ['ent_cat', None, None, None, '5', catranges_ent, None, None, None]
var_r24 =    [3, 'r24', catranges_r24]
cpt_r24 =    ['rain_24_cat', 'ent_cat', 'rain_48_cat',None, '5', catranges_r24, catranges_ent, catranges_r48,None]
var_r48 =    [4, 'r48', catranges_r48]
cpt_r48 =    ['rain_48_cat', 'ent_cat', 'Rintensity_cat',None, '5', catranges_r48, catranges_ent, catranges_rint,None]
var_r72 =    [5, 'r72', catranges_r72]
cpt_r72 =    ['rain_72_cat', 'ent_cat', 'EC_cat',None, '4', catranges_r72, catranges_ent, catranges_ecc,None]
var_r7d =    [6, 'r7d', catranges_r7d]
cpt_r7d =    ['rain_7d_cat', 'ent_cat', 'days_after_rain_20mm_cat','EC_cat', '3', catranges_r7d, catranges_ent, catranges_dar20, catranges_ecc]
var_rdis =   [7, 'rdis', catranges_rdis]
cpt_rdis =   ['Rdistribution_cat', 'ent_cat', None, None, '3', catranges_rdis, catranges_ent,None,None]
var_so24 =   [8, 'so24', catranges_so24]
cpt_so24 =   ['solar_24_cat', 'ent_cat', None, None, '4', catranges_so24, catranges_ent, None,None]
var_ecc =    [9, 'ecc', catranges_ecc]
cpt_ecc =    ['EC_cat', 'ent_cat', None, None, '4', catranges_ecc, catranges_ent, None,None]
var_rint =   [10, 'rint', catranges_rint]
cpt_rint =   ['Rintensity_cat', 'ent_cat', None, None, '4', catranges_rint, catranges_ent, None,None]
var_stopct = [11, 'stopct', catranges_ctopct]
cpt_stopct = ['stormwater_pct_cat', 'ent_cat', 'EC_cat',None, '4', catranges_ctopct, catranges_ent, catranges_ecc,None]
var_dar20 =  [12, 'dar20', catranges_dar20]
cpt_dar20 =  ['days_after_rain_20mm_cat', 'ent_cat', None, None, '5', catranges_dar20, catranges_ent, None, None]

#---------------------create nodes function---------------------------
def createnode(train_datasets,var_xx, cpt_xx):
    node=BbnNode(Variable(var_xx[0], var_xx[1], var_xx[2]),
                 cpt(train_datasets, child=cpt_xx[0], parent1=cpt_xx[1], parent2=cpt_xx[2], parent3=cpt_xx[3], 
                     child_cat_number=cpt_xx[4],chcat=cpt_xx[5], pcat1=cpt_xx[6], pcat2=cpt_xx[7],pcat3=cpt_xx[8])
                )
    return node

#---------------------create nodes---------------------------
train_data=df_train
su24=createnode(train_data,var_su24,cpt_su24)
rdur=createnode(train_data,var_rdur,cpt_rdur)
ent=createnode(train_data,var_ent,cpt_ent)
r24=createnode(train_data,var_r24,cpt_r24)
r48=createnode(train_data,var_r48,cpt_r48)
r72=createnode(train_data,var_r72,cpt_r72)
r7d=createnode(train_data,var_r7d,cpt_r7d)
rdis=createnode(train_data,var_rdis,cpt_rdis)
so24=createnode(train_data,var_so24,cpt_so24)
ecc=createnode(train_data,var_ecc,cpt_ecc)
rint=createnode(train_data,var_rint,cpt_rint)
stopct=createnode(train_data,var_stopct,cpt_stopct)
dar20=createnode(train_data,var_dar20,cpt_dar20)

#------------------ nodes loggas, excgas and pollu are for the intepretation from ent prediction to pollution----------------------
loggas = BbnNode(Variable(13, 'loggas',['-2.3561', '-2.3561--2', '-2--1.7', '-1.7--1.5', '-1.5--1.2', '-1.2--0.9', '-0.9--0.5','-0.5-0', '0-1', '1-3', '3-4', '4-27']),
                          [99.89, 0.00998801, 0.00998801, 0.00998801, 0.00998801, 0.00998801, 0.00998801, 0.00998801, 0.00998801,0.00998801, 0.00998801, 0.00998801,
                          0.00998801, 44.6564, 55.2437, 0.00998801, 0.00998801, 0.00998801, 0.00998801, 0.00998801, 0.00998801,0.00998801, 0.00998801, 0.00998801,
                          0.00998801, 0.00998801, 30.2038, 62.535, 7.17139, 0.00998801, 0.00998801, 0.00998801, 0.00998801,0.00998801, 0.00998801, 0.00998801,
                          0.00998801, 0.00998801, 0.00998801, 0.00998801, 12.8046, 17.9285, 30.833, 38.354, 0.00998801,0.00998801, 0.00998801, 0.00998801,
                          0.00998802, 0.00998802, 0.00998802, 0.00998802, 0.00998802, 0.00998802, 0.00998802, 9.97803, 65.2117,24.3308, 0.399521, 0.00998802])

excgas = BbnNode(Variable(14, 'excgas',['<0.01', '0.01-0.03', '0.03-0.05', '0.05-0.1', '0.1-0.2', '0.2-0.4', '0.4', '0.4-0.7','0.7-0.9', '>0.9']),
                          [99.91, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001,0.00999001,
                          # 99.9101,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,
                          33.6464, 59.3507, 6.93307, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001,0.00999001,
                          # 33.6464,59.2008,7.08292,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 51.8082, 48.1119, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001,0.00999001,
                          # 0.00999001,0.00999001,51.4186,48.5015,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 99.9101, 0.00999001, 0.00999001, 0.00999001, 0.00999001,0.00999001, 0.00999001,
                          # 0.00999001,0.00999001,0.00999001,99.9101,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 9.65035, 90.2697, 0.00999001, 0.00999001, 0.00999001, 0.00999001,0.00999001,
                          # 0.00999001,0.00999001,0.00999001,8.79121,91.1289,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 95.8442, 4.07592, 0.00999001, 0.00999001, 0.00999001,0.00999001,
                          # 0.00999001,0.00999001,0.00999001,0.00999001,95.9441,3.97602,0.00999001,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 99.9101, 0.00999001, 0.00999001,0.00999001, 0.00999001,
                          # 0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,99.9101,0.00999001,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 89.6004, 0.01998, 10.3097, 0.00999001,0.00999001,
                          # 0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,89.4506,10.4695,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 99.9101,0.00999001, 0.00999001,
                          # 0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,99.9101,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 15.2947, 84.6254,0.00999001,
                          # 0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,99.9101,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001,99.9101, 0.00999001,
                          # 0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,99.9101,0.00999001,0.00999001,0.00999001,
                          0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001, 0.00999001,1.27872, 98.6414])
                          # 0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,0.00999001,99.9101,0.00999001,0.00999001,0.00999001])

pollu = BbnNode(Variable(15,'pollu', ['Unlikely', 'Possible', 'Likely']), [99.98, 0.009997, 0.009997,
                                                                            99.98, 0.009997, 0.009997,
                                                                            0.009997, 99.98, 0.009997,
                                                                            0.009997, 99.98, 0.009997,
                                                                            0.009997, 0.009997, 99.98,
                                                                            0.009997, 0.009997, 99.98,
                                                                            0.009997, 0.009997, 99.98,
                                                                            0.009997, 0.009997, 99.98,
                                                                            0.009997, 0.009997, 99.98,
                                                                            0.009997, 0.009997, 99.98])





# COMMAND ----------

# MAGIC %md ## Creat Bayesian Network

# COMMAND ----------

def BayesianNetwork(su24,rdur,ent,r24,r48,r72,r7d,rdis,so24,ecc,rint,stopct,dar20,loggas,excgas,pollu):
    bbn = (Bbn() 
        .add_node(su24) 
        .add_node(rdur) 
        .add_node(ent) 
        .add_node(r24) 
        .add_node(r48) 
        .add_node(r72) 
        .add_node(r7d) 
        .add_node(rdis) 
        .add_node(so24) 
        .add_node(ecc) 
        .add_node(rint) 
        .add_node(stopct) 
        .add_node(dar20) 
        .add_node(loggas) 
        .add_node(excgas) 
        .add_node(pollu) 
        .add_edge(Edge(r48, r24, EdgeType.DIRECTED)) 
        .add_edge(Edge(rint, r48, EdgeType.DIRECTED)) 
        .add_edge(Edge(rint, rdur, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, rdis, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, r24, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, r48, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, rint, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, su24, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, rdur, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, so24, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, dar20, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, r72, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, ecc, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, stopct, EdgeType.DIRECTED)) 
        .add_edge(Edge(dar20, r7d, EdgeType.DIRECTED)) 
        .add_edge(Edge(ecc, stopct, EdgeType.DIRECTED)) 
        .add_edge(Edge(ecc, r72, EdgeType.DIRECTED)) 
        .add_edge(Edge(so24, su24, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, r7d, EdgeType.DIRECTED)) 
        .add_edge(Edge(ecc, r7d, EdgeType.DIRECTED)) 
        .add_edge(Edge(ent, loggas, EdgeType.DIRECTED)) 
        .add_edge(Edge(loggas, excgas, EdgeType.DIRECTED)) 
        .add_edge(Edge(excgas, pollu, EdgeType.DIRECTED))
          )
    join_tree = InferenceController.apply(bbn)
    return bbn,join_tree

bbn,BayesianNet=BayesianNetwork(su24,rdur,ent,r24,r48,r72,r7d,rdis,so24,ecc,rint,stopct,dar20,loggas,excgas,pollu)
# BayesianNet.
#       To add evidence of events that happened so probability distribution can be recalculated
def evidence(ev, nod, cat, val):
    ev = (EvidenceBuilder()
          .with_node(BayesianNet.get_bbn_node_by_name(nod))
          .with_evidence(cat, val)
          .build()
         )
    BayesianNet.set_observation(ev)
# BayesianNet.get_unobserved_evidence

# COMMAND ----------

# MAGIC %md ## Create class and Wrapped model

# COMMAND ----------

# # ---------Two lists are for prediction categorisation ------------
pollution_prediction_cat = ['Unlikely', 'Possible', 'Likely']
ent_prediction_cat = ['0.0-32', '1.32-39', '2.39-51', '3.51-158', '4.>=158']
pollution_prediction = []

# # =================================== Create the class Wrapper for BN model==========================
class BayesianNetWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, BayesianNet):#load the bayesian model CPT etc
        self.model = BayesianNet 
    
    def predict(self, context, model_input):
        outputs = model_input.iloc[:,0:1]
#         outputs['prediction'] = None
        for index, model_input in model_input.iterrows():
            self.model.unobserve_all()
            # update evidence for all features
            evidence('ev1', 'rdur', model_input.Rduration_cat, 1)
            evidence('ev2', 'rint', model_input.Rintensity_cat, 1)
            evidence('ev3', 'r48', model_input.rain_48_cat, 1)
            evidence('ev4', 'rdis', model_input.Rdistribution_cat, 1)
            evidence('ev5', 'r24', model_input.rain_24_cat, 1)
            evidence('ev6', 'r72', model_input.rain_72_cat, 1)
            evidence('ev7', 'r7d', model_input.rain_7d_cat, 1)
            evidence('ev10', 'dar20', model_input.days_after_rain_20mm_cat, 1)
            if pd.isnull(model_input.sun_24_cat) and pd.isnull(model_input.solar_24_cat):
                pass
            elif pd.isnull(model_input.sun_24_cat) and pd.notna(model_input.solar_24_cat):
                evidence('ev8', 'so24', model_input.solar_24_cat, 1)
            elif pd.isnull(model_input.solar_24_cat) and pd.notna(model_input.sun_24_cat):
                evidence('ev9', 'su24', model_input.sun_24_cat, 1)
            elif pd.notna(model_input.sun_24_cat) and pd.notna(model_input.solar_24_cat):
                evidence('ev8', 'so24', model_input.solar_24_cat, 1)
                evidence('ev9', 'su24', model_input.sun_24_cat, 1)
            
            #------ prediction of pollution ---------------
            pollu_inference = [self.model.get_bbn_potential(pollu).entries[0].value,
                           self.model.get_bbn_potential(pollu).entries[1].value,
                           self.model.get_bbn_potential(pollu).entries[2].value]
            max_index_pollu = pollu_inference.index(max(pollu_inference))
            pollu_pridict_proba = pollu_inference[max_index_pollu]
            outputs.loc[index,'prediction'] = pollution_prediction_cat[max_index_pollu]
        
        # ------------ create prediction dataframe for output----------
        d = {'pollu_cato': outputs['prediction'].values.tolist()}
        prediction = pd.DataFrame(data=d)
        return prediction

wrappedModel = BayesianNetWrapper(BayesianNet)    
# # ===================================End Create the class Wrapper for BN model==========================

# # # ----------------------------- inference to obtain output schema for registering the model into MLflow--------------------------
# infer_input_cato = spark.table("datalab.riverwatch_preprocessed_model_input").limit(10)
# inputdata=(infer_input_cato
#             .select("Rduration_cat",
#                     "Rintensity_cat", 
#                     "rain_48_cat",
#                     "Rdistribution_cat",
#                     "rain_24_cat",
#                     "rain_72_cat",
#                     "rain_7d_cat",
#                     "solar_24_cat",
#                     "sun_24_cat",
#                     "days_after_rain_20mm_cat")
#             .limit(10)
#             .toPandas()
#         )
data = [
    [1,
     "Bayview", 
     '2022-02-10T07:00:00.000+0000', 
     '1.0-6',
     '1.0-6', 
     '1.0-14',
     '1.10-50',
     '1.0-2',
     '1.0-2',
     '1.0-5',
     '0.0-1',
     '0.0-1',
     '0.0-1'
    ]
]
df = (spark.createDataFrame(data, ["locationId", 
                                  "siteName", 
                                  "timestamp", 
                                  "rain_24_cat", 
                                  "rain_48_cat", 
                                  "rain_72_cat",
                                  "rain_7d_cat",
                                  "Rintensity_cat",
                                  "Rduration_cat",
                                  "Rdistribution_cat",
                                  "days_after_rain_20mm_cat",
                                  "sun_24_cat",
                                  "solar_24_cat"
                                 ])
      .withColumn("locationId", psf.col("locationId").cast("INT"))
     )

prediction=(wrappedModel.predict(None,df.toPandas()))
display(prediction)


# COMMAND ----------

# MAGIC %md ## Register the model as udf with predefined format of input and output

# COMMAND ----------

signature = mlflow.models.signature.infer_signature(df.toPandas(), prediction) 

# COMMAND ----------

conda_environment = {
    "name": "mlflow-env",
    "channels": ["conda-forge"],
    "dependencies": [
        "python=3.8.10",
        "pip<=21.0.1",
        
        {
            "pip": [
                "mlflow==1.30.0",
                "cloudpickle==2.2.0",
                "pybbn==3.2.1"
            ],
        },
    ],
}


# COMMAND ----------

_MODEL_NAME = "riverwatch-pollution-classifier"
_MODEL_DESCRIPTION = 'Water quality prediction model using bayesian network to predict the likelihood of pollution occurring at a particular swim site. The bayesian network predicts entreccocci levels which is converted into a pollution likelihood.'

mlflow_experiment = mlflow.get_experiment_by_name("/AI-ML/Riverwatch/riverwatch-pollution-classifier")

if mlflow_experiment == None:
    experiment_id = mlflow.create_experiment("/AI-ML/Riverwatch/riverwatch-pollution-classifier")
else:
    experiment_id = mlflow_experiment.experiment_id
    
with mlflow.start_run(run_name="riverwatch-pollution-classifier", experiment_id=experiment_id, description=_MODEL_DESCRIPTION):
    mlflow.set_tag("Release Version", "1.0")
    mlflow.set_tag("Application", "Riverwatch")
    model_details = mlflow.pyfunc.log_model(_MODEL_NAME, 
                                            python_model=wrappedModel,  
                                            conda_env=conda_environment,
                                            registered_model_name=_MODEL_NAME,
                                            signature=signature
                                           )

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

model_details = client.get_latest_versions(name=_MODEL_NAME, stages=["None"])

client.update_model_version(
    name=_MODEL_NAME,
    version=model_details[0].version,
    description=_MODEL_DESCRIPTION
)

client.update_registered_model(
    name=_MODEL_NAME,
    description=_MODEL_DESCRIPTION
)

client.set_registered_model_tag(
  name=_MODEL_NAME,
  key="Application",
  value="Riverwatch"
)

client.set_model_version_tag(
  name=_MODEL_NAME,
  version=model_details[0].version,
  key="Application",
  value="Riverwatch"
)

# COMMAND ----------

client.transition_model_version_stage(
    name=_MODEL_NAME,
    version=model_details[0].version,
    stage='Production'
)
