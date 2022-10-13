# Databricks notebook source
# MAGIC %md # This notebook needs cluster "riverwatch" for BN model library

# COMMAND ----------

# MAGIC %md # Import libraries

# COMMAND ----------

# ----------------    Spark libraries---------------------------------
from pyspark.sql import functions as psf
from pyspark.sql import Window as W
from pyspark.sql import types as t
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, FloatType

# ------------------ Below are libraries for BN model, need to run using cluster "riverwatch" -----------------

import pandas as pd  # for data manipulation
import numpy as np
import networkx as nx  # for drawing graphs
import matplotlib.pyplot as plt  # for drawing graphs
from mlxtend.plotting import plot_confusion_matrix
from pathlib import Path
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
import swat

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
import numpy as np
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from mlflow.models.signature import infer_signature
from mlflow.utils.environment import _mlflow_conda_env
import cloudpickle
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
df_train=pd.read_csv("/dbfs/FileStore/DataLab/Riverwatch/df_train_bef2014.csv")
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
            evidence('ev1', 'rdur', model_input.Rduration_cat, 1)
            evidence('ev2', 'rint', model_input.Rintensity_cat, 1)
            evidence('ev3', 'r48', model_input.rain_48_cat, 1)
            evidence('ev4', 'rdis', model_input.Rdistribution_cat, 1)
            evidence('ev5', 'r24', model_input.rain_24_cat, 1)
            evidence('ev6', 'r72', model_input.rain_72_cat, 1)
            evidence('ev7', 'r7d', model_input.rain_7d_cat, 1)
            evidence('ev8', 'so24', model_input.solar_24_cat, 1)
            evidence('ev9', 'su24', model_input.sun_24_cat, 1)
            evidence('ev10', 'dar20', model_input.days_after_rain_20mm_cat, 1)
            evidence('ev10', 'dar20', model_input.days_after_rain_20mm_cat, 1)
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

# # ----------------------------- inference to obtain output schema for registering the model into MLflow--------------------------
infer_input_cato = spark.table("datalab.riverwatch_preprocessed_model_input").limit(10)
inputdata=(infer_input_cato
            .select("Rduration_cat",
                    "Rintensity_cat", 
                    "rain_48_cat",
                    "Rdistribution_cat",
                    "rain_24_cat",
                    "rain_72_cat",
                    "rain_7d_cat",
                    "solar_24_cat",
                    "sun_24_cat",
                    "days_after_rain_20mm_cat")
            .limit(10)
            .toPandas()
        )

prediction=(wrappedModel.predict(None,inputdata))
# display(prediction)


# COMMAND ----------

# MAGIC %md ## Check the address of the model

# COMMAND ----------

wrappedModel.model

# COMMAND ----------

# MAGIC %md ## Register the model as udf with predefined format of input and output

# COMMAND ----------

signature = infer_signature(inputdata, prediction) 
# mlflow.sklearn.log_model(clf, "iris_rf", signature=signature)
mlflow.pyfunc.log_model("riverwatch_test", python_model=wrappedModel,signature=signature)

# COMMAND ----------

# MAGIC %md # Apendices

# COMMAND ----------

# MAGIC %md ## Test data categorisation
# MAGIC 
# MAGIC The testing data is a spark dataframe

# COMMAND ----------

# dawnfraser_test=spark.read.option("header",True).csv("/FileStore/DataLab/Riverwatch/df_test_aft2014.csv")

# infer_input_cato= (dawnfraser_test
#                        .withColumn("rain_24_cat", psf.when(psf.col("rain_24") == 0, catranges_r24[0])
#                                                      .when(((psf.col("rain_24") > 0) 
#                                                             & (psf.col("rain_24") < 6)), catranges_r24[1])
#                                                      .when(((psf.col("rain_24") >= 6) 
#                                                             & (psf.col("rain_24") < 12)), catranges_r24[2])
#                                                      .when(((psf.col("rain_24") >= 12) 
#                                                             & (psf.col("rain_24") < 20)), catranges_r24[3])
#                                                      .when((psf.col("rain_24") >= 20), catranges_r24[4])
#                                   )
#                        .withColumn("rain_48_cat", psf.when(psf.col("rain_48") == 0, catranges_r48[0])
#                                                      .when(((psf.col("rain_48") > 0) 
#                                                             & (psf.col("rain_48") < 6)), catranges_r48[1])
#                                                      .when(((psf.col("rain_48") >= 6) 
#                                                             & (psf.col("rain_48") < 14)), catranges_r48[2])
#                                                      .when(((psf.col("rain_48") >= 14) 
#                                                             & (psf.col("rain_48") < 20)), catranges_r48[3])
#                                                      .when((psf.col("rain_48") >= 20), catranges_r48[4])
#                                   )
#                        .withColumn("rain_72_cat", psf.when(psf.col("rain_72") == 0, catranges_r72[0])
#                                                      .when(((psf.col("rain_72") > 0) 
#                                                             & (psf.col("rain_72") < 14)), catranges_r72[1])
#                                                      .when(((psf.col("rain_72") >= 14) 
#                                                             & (psf.col("rain_72") < 30)), catranges_r72[2])
#                                                      .when((psf.col("rain_72") >= 30), catranges_r72[3])
#                                   )
#                        .withColumn("rain_7d_cat", psf.when(((psf.col("rain_7d") >= 0) 
#                                                             & (psf.col("rain_7d") < 10)), catranges_r7d[0])
#                                                      .when(((psf.col("rain_7d") >= 10) 
#                                                             & (psf.col("rain_7d") < 50)), catranges_r7d[1])
#                                                      .when((psf.col("rain_7d") >= 50), catranges_r7d[2])
#                                   )
#                        .withColumn("Rintensity_cat", psf.when(psf.col("Rintensity") == 0, catranges_rint[0])
#                                                      .when(((psf.col("Rintensity") > 0) 
#                                                             & (psf.col("Rintensity") < 2)), catranges_rint[1])
#                                                      .when(((psf.col("Rintensity") >= 2) 
#                                                             & (psf.col("Rintensity") < 4)), catranges_rint[2])
#                                                      .when((psf.col("Rintensity") >= 4), catranges_rint[3])
#                                   )
#                        .withColumn("Rduration_cat", psf.when(psf.col("Rduration") == 0, catranges_rdur[0])
#                                                      .when(((psf.col("Rduration") > 0) 
#                                                             & (psf.col("Rduration") < 2)), catranges_rdur[1])
#                                                      .when(((psf.col("Rduration") >= 2) 
#                                                             & (psf.col("Rduration") < 4)), catranges_rdur[2])
#                                                      .when((psf.col("Rduration") >= 4), catranges_rdur[3])
#                                   )
#                        .withColumn("Rdistribution_cat", psf.when(psf.col("Rdistribution") == 0, catranges_rdis[0])
#                                                      .when(((psf.col("Rdistribution") > 0) 
#                                                             & (psf.col("Rdistribution") < 7)), catranges_rdis[1])
#                                                      .when((psf.col("Rdistribution") >= 7), catranges_rdis[2])
#                                   )
#                        .withColumn("sun_24_cat", psf.when(((psf.col("sun_24") >= 0) 
#                                                            & (psf.col("sun_24") < 1)), catranges_su24[0])
#                                                      .when(((psf.col("sun_24") >= 1) 
#                                                             & (psf.col("sun_24") < 5)), catranges_su24[1])
#                                                      .when((psf.col("sun_24") >= 5), catranges_su24[2])
#                                   )
#                         .withColumn("solar_24_cat", psf.when(((psf.col("solar_24") >= 0) 
#                                                               & (psf.col("solar_24") < 1)), catranges_so24[0])
#                                                      .when(((psf.col("solar_24") >= 1) 
#                                                             & (psf.col("solar_24") < 4)), catranges_so24[1])
#                                                      .when(((psf.col("solar_24") >= 4) 
#                                                             & (psf.col("solar_24") < 9)), catranges_so24[2])
#                                                      .when((psf.col("solar_24") >= 9), catranges_so24[3])
#                                   )
#                         .withColumn("days_after_rain_20mm_cat", psf.when(((psf.col("days_after_rain_20mm") >= 0) 
#                                                                           & (psf.col("days_after_rain_20mm") < 1)), catranges_dar20[0])
#                                                                     .when(((psf.col("days_after_rain_20mm") >= 1) 
#                                                                                 & (psf.col("days_after_rain_20mm") < 2)), catranges_dar20[1])
#                                                                     .when(((psf.col("days_after_rain_20mm") >= 2) 
#                                                                                 & (psf.col("days_after_rain_20mm") < 3)), catranges_dar20[2])
#                                                                     .when(((psf.col("days_after_rain_20mm") >= 3) 
#                                                                                 & (psf.col("days_after_rain_20mm") < 4)), catranges_dar20[3])
#                                                                     .when((psf.col("days_after_rain_20mm") >= 4), catranges_dar20[4])
#                                    )    
               
#                 )
# display(infer_input_cato
#        .orderBy("Date"))

# COMMAND ----------

# MAGIC %md ## Test registered model

# COMMAND ----------

# import mlflow
# from pyspark.sql.functions import struct

# # # ------------------------------- load experiment model-------------
# # logged_model = 'runs:/88146961206043bc86734a9771350843/riverwatch_test'

# # # --------------------------------load registered model--------------
# _MODEL_NAME = "riverwatch-pollution-classifier"
# _MODEL_VERSION = "2"
# logged_model = f'models:/{_MODEL_NAME}/{_MODEL_VERSION}'
# # # --------------------------------end load registered model--------------

# # Load model as a Spark UDF. Override result_type if the model does not return double values.
# loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, env_manager='conda', result_type='string')
# # df = df.withColumn("MyNewCol", expr("udf.Method(LGACode, LGA, )"))
# expr_lastrow = [psf.last(col).alias(col) for col in infer_input_cato.columns] # This is for select the last row of the dataframe
# w_severalbottom = W().orderBy(psf.lit('epoch_timestamp')) # This is for select several bottom rows of the dataframe
# tmpdata=(test_BP_PP
# #          .agg(*expr_lastrow)
# #          .withColumn("row_num",psf.row_number().over(w_severalbottom))
# #          .filter(psf.col("row_num").between(1,5))
#          .select("rain_48","Rduration_cat","Rintensity_cat", "rain_48_cat","Rdistribution_cat","rain_24_cat",
#                  "rain_72_cat","rain_7d_cat","solar_24_cat","sun_24_cat","days_after_rain_20mm_cat")
#         )
# # w = Window().partitionBy(lit('a')).orderBy(lit('a'))

# # df1 = df.withColumn("row_num", row_number().over(w))

# # df1.filter(col("row_num").between(1,2)).show()  
# # display(tmpdata)

# inputdata=struct(tmpdata.columns)
# # print(inputdata)
# prediction=(tmpdata
#             .withColumn('predict_BN',loaded_model(inputdata)
#                        )
#             .withColumn("predict_BW",psf.when(((psf.col("rain_48") >= 0) 
#                                           & (psf.col("rain_48") < 12)), "Unlikely")
#                                             .when(((psf.col("rain_48") >= 12) 
#                                           & (psf.col("rain_48") < 20)), "Possible")
#                                             .when((psf.col("rain_48") >= 20), "Likely")
#                        )
#             )
# display(prediction)
# loaded_model

# COMMAND ----------

# MAGIC %md ## Signature example

# COMMAND ----------

# import pandas as pd
# from sklearn import datasets
# from sklearn.ensemble import RandomForestClassifier
# import mlflow
# import mlflow.sklearn
# from mlflow.models.signature import infer_signature

# iris = datasets.load_iris()
# # print(iris) #Cannot call display(<class 'sklearn.utils._bunch.Bunch'>)
# iris_train = pd.DataFrame(iris.data, columns=iris.feature_names)
# display(iris_train) #Dataframe
# clf = RandomForestClassifier(max_depth=7, random_state=0)
# # print(clf)
# clf.fit(iris_train, iris.target)
# signature = infer_signature(iris_train, clf.predict(iris_train))
# print(signature) # Cannot call display(<class 'mlflow.models.signature.ModelSignature'>)
# # mlflow.sklearn.log_model(clf, "iris_rf", signature=signature)

# COMMAND ----------

# df_test=pd.read_csv("/dbfs/FileStore/DataLab/Riverwatch/df_test_aft2014.csv")
# # df_test=dawnfraser_test
# # display(df_test)
# df_test['ent_cat'] = df_test['ent'].apply(
#     lambda x: '0.0-32' if 0 <= x < 32 else '1.32-39' if 32 <= x < 39 else '2.39-51' if 39 <= x < 51 else '3.51-158' if 51 <= x < 158 else '4.>=158')
# df_test['EC_cat'] = df_test['EC'].apply(
#     lambda x: '0.0-40' if 0 <= x < 40 else '1.40-45' if 40 <= x < 45 else '2.45-50' if 45 <= x < 50 else '3.>=50')
# df_test['solar_24_cat'] = df_test['solar_24'].apply(
#     lambda x: '0.0-1' if 0 <= x < 1 else '1.1-4' if 1 <= x < 4 else '2.4-9' if 4 <= x < 9 else '3.>=9')
# df_test['sun_24_cat'] = df_test['sun_24'].apply(
#     lambda x: '0.0-1' if 0 <= x < 1 else '1.1-5' if 1 <= x < 5 else '2.>=5')
# df_test['rain_24_cat'] = df_test['rain_24'].apply(
#     lambda x: '0.0' if x == 0 else '1.0-6' if 0 < x < 6 else '2.6-12' if 6 <= x < 12 else '3.12-20' if 12 <= x < 20 else '4.>=20')
# df_test['rain_48_cat'] = df_test['rain_48'].apply(
#     lambda x: '0.0' if x == 0 else '1.0-6' if 0 < x < 6 else '2.6-14' if 6 <= x < 14 else '3.14-20' if 14 <= x < 20 else '4.>=20')
# df_test['rain_72_cat'] = df_test['rain_72'].apply(
#     lambda x: '0.0' if x == 0 else '1.0-14' if 0 < x < 14 else '2.14-30' if 14 <= x < 30 else '3.>=30')
# df_test['rain_7d_cat'] = df_test['rain_7d'].apply(
#     lambda x: '0.0-10' if 0 <= x < 10 else '1.10-50' if 10 <= x < 50 else '2.>=50')
# df_test['Rintensity_cat'] = df_test['Rintensity'].apply(
#     lambda x: '0.0' if x == 0 else '1.0-2' if 0 < x < 2 else '2.2-4' if 2 <= x < 4 else '3.>=4')
# df_test['Rduration_cat'] = df_test['Rduration'].apply(
#     lambda x: '0.0' if x == 0 else '1.0-2' if 0 < x < 2 else '2.2-4' if 2 <= x < 4 else '3.>=4')
# df_test['Rdistribution_cat'] = df_test['Rdistribution'].apply(
#     lambda x: '0.0' if x == 0 else '1.0-5' if 0 < x < 7 else '2.>=7')
# df_test['stormwater_pct_cat'] = df_test['stormwater_pct'].apply(
#     lambda x: '0.0-5' if 0 <= x < 5 else '1.5-15' if 5 <= x < 15 else '2.15-25' if 15 <= x < 25 else '3.>=25')
# df_test['days_after_rain_20mm_cat'] = df_test['days_after_rain_20mm'].apply(
#     lambda x: '0.0-1' if 0 <= x < 1 else '1.1-2' if 1 <= x < 2 else '2.2-3' if 2 <= x < 3 else '3.3-4' if 3 <= x < 4 else '4.>=4')               
#     #-------------------------------- Model Inference
    
# # evidence('ev1', 'rdur', df_test.Rduration_cat[0], 1)
# # evidence('ev2', 'rint', df_test.Rintensity_cat[0], 1)
# # evidence('ev3', 'r48', df_test.rain_48_cat[0], 1)
# # evidence('ev4', 'rdis', df_test.Rdistribution_cat[0], 1)
# # evidence('ev5', 'r24', df_test.rain_24_cat[0], 1)
# # evidence('ev6', 'r72', df_test.rain_72_cat[0], 1)
# # evidence('ev7', 'r7d', df_test.rain_7d_cat[0], 1)
# # evidence('ev8', 'so24', df_test.solar_24_cat[0], 1)
# # evidence('ev9', 'su24', df_test.sun_24_cat[0], 1)
# # # evidence('ev10', 'dar20', df_test.days_after_rain_20mm_cat[0], 1)
# # pollu_inference = [BayesianNet.get_bbn_potential(pollu).entries[0].value,
# #                    BayesianNet.get_bbn_potential(pollu).entries[1].value,
# #                    BayesianNet.get_bbn_potential(pollu).entries[2].value]
# # max_index = pollu_inference.index(max(pollu_inference))
# # print(pollu_inference)

# # print(BNprediction(df_test.Rduration_cat[1],
# #                    df_test.Rintensity_cat[1],
# #                    df_test.rain_48_cat[1],
# #                    df_test.Rdistribution_cat[1],
# #                    df_test.rain_24_cat[1],
# #                    df_test.rain_72_cat[1],
# #                    df_test.rain_7d_cat[1],
# #                    df_test.solar_24_cat[1],
# #                    df_test.sun_24_cat[1],
# #                    df_test.days_after_rain_20mm_cat[1]
# #                   ))

# tmpdata=(infer_input_cato
#                 .where(psf.col("epoch_timestamp")==1658102400)
#                 .select("Rduration_cat","Rintensity_cat",
#                         "rain_48_cat","Rdistribution_cat",
#                         "rain_24_cat","rain_72_cat",
#                         "rain_7d_cat","solar_24_cat",
#                         "sun_24_cat","days_after_rain_20mm_cat")
#             .toPandas()
#                )
# prediction=(BNprediction(tmpdata.Rduration_cat[0],
#                                  tmpdata.Rintensity_cat[0],
#                                   tmpdata.rain_48_cat[0],
#                                   tmpdata.Rdistribution_cat[0],
#                                   tmpdata.rain_24_cat[0],
#                                   tmpdata.rain_72_cat[0],
#                                   tmpdata.rain_7d_cat[0],
#                                   tmpdata.solar_24_cat[0],
#                                   tmpdata.sun_24_cat[0],
#                                   tmpdata.days_after_rain_20mm_cat[0])

#            )
# print(prediction)
# # BayesianNet.

# COMMAND ----------

# MAGIC %md ## Plot BN model

# COMMAND ----------

# # Set node positions
# pos = {0: (1, 4), 1: (4, 4), 2: (2.5, 3.5), 3: (2, 5), 4: (3, 5), 5: (2, 2), 6: (1, 1), 7: (1, 5), 8: (1, 3), 9: (3, 1),
#        10: (4, 5), 11: (4, 1), 12: (1, 2), 13: (4, 2.5), 14: (5, 2.5), 15: (5, 1)}

# # Set options for graph looks
# options = {
#     "font_size": 12,
#     "node_size": 2000,
#     "node_color": "white",
#     "edgecolors": "black",
#     "edge_color": "red",
#     "linewidths": 3,
#     "width": 3, }

# # Generate graph
# n, d = bbn.to_nx_graph()
# nx.draw(n, with_labels=True, labels=d, pos=pos, **options)

# # Update margins and print the graph
# ax = plt.gca()
# ax.margins(0.10)
# plt.axis("off")
# plt.show()
