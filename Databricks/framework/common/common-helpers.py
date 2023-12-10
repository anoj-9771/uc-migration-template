# Databricks notebook source
import json
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def get_env() -> str:
    """centralised function to get prefix for the environment's catalogs"""
    rg_id = dbutils.secrets.get('ADS', 'databricks-workspace-resource-id')

    if 'dev' in rg_id:
        return 'dev_'
    elif 'test' in rg_id:
        return 'test_'
    elif 'preprod' in rg_id:
        return 'ppd_'
    elif 'prod' in rg_id:
        return ''
    else:
        raise Exception
