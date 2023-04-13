# Databricks notebook source
from mlflow.store.artifact.databricks_models_artifact_repo import DatabricksModelsArtifactRepository

def get_model_signed_urls(model_name, stage):
    """
    Given a model name and stage, returns an array of tuples of the form (path, signed_url)
    where `path` is the path of a model file within a model directory (e.g. 'model.pkl', 'MLmodel')
    and `signed_url` is a corresponding presigned URL for downloading the file.
    
    The returned array can be used with download_model_from_signed_urls to download a model
    to a local directory
    """
    model_artifact_repo = DatabricksModelsArtifactRepository(f"models:/{model_name}/{stage}")
    model_artifacts = model_artifact_repo.list_artifacts("")
    model_artifacts = list(filter(lambda file: file.path != 'artifacts', model_artifacts))
    model_artifacts.append(model_artifact_repo.list_artifacts(path='artifacts')[0])
    
    
    return [(model_artifact.path, model_artifact_repo._get_signed_download_uri(model_artifact.path)) for model_artifact in model_artifacts]

# COMMAND ----------

from mlflow.utils.file_utils import download_file_using_http_uri
import os

def download_model_from_signed_urls(paths_and_urls, directory):
    """
    Downloads a set of model files (each represented by their own presigned URLs)
    to the specified directory
    
    @param paths_and_urls: Array of tuples of the form (path, signed_url) corresponding to model files
    @param directory: Destination directory
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        if not os.path.exists(f"{directory}/artifacts"):
            os.makedirs(f"{directory}/artifacts")
    
    for path, signed_url in paths_and_urls:
        print(path)
        dest_path = os.path.join(directory, path)
        download_file_using_http_uri(http_uri=signed_url, download_path=dest_path)

# COMMAND ----------

import tempfile
import shutil

def load_model_from_signed_urls(paths_and_urls):
    """
    Helper for loading a model from signed URLs, which can either be
    fetched from cache or the REST API
    """
    # Use the signed URLs to download the model to a temporary local directory
#     temp_dir = f"/dbfs{tempfile.mkdtemp()}"
    temp_dir = "/dbfs/mnt/blob-sewercctvmodel/mlflow artifacts"
    try:
        download_model_from_signed_urls(paths_and_urls, temp_dir)
        return mlflow.pyfunc.load_model(temp_dir)
    finally:
        # Clean up the tempdir after the model is loaded
        print("Saving model to mlflow artifacts")
#         shutil.rmtree(temp_dir)

# COMMAND ----------

import mlflow
import time
# In practice, use a shared cache across containers
_CACHE = {} 

def load_model(model_name, stage):
    """
    Load the latest version of the specified model in the specified stage
    """
    global _CACHE
    cache_key = (model_name, stage)
    if cache_key in _CACHE:
        (signed_urls, expiry) = _CACHE[cache_key]
        # Cache expiry may be handled by a real caching solution
        if expiry < time.time():
            return load_model_from_signed_urls(signed_urls)
    # If unable to fetch unexpired signed URLs for the model, fetch new presigned URLs
    # and add them to the cache, setting expiry to 10 min from now
    # (presigned URLs expire in 15 min but we allow some buffer)
    signed_urls = get_model_signed_urls(model_name, stage)
    _CACHE[cache_key] = (signed_urls, time.time() + 10 * 600)
    return load_model_from_signed_urls(signed_urls)

# COMMAND ----------

model_name = "sewer-cctv-image-classifier"
stage = "Production"
loaded_model = load_model(model_name, stage)

# COMMAND ----------


