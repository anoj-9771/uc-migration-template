# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor
class NotebookData:
  def __init__(self, path, timeout, parameters=None, retry=0):
    self.path = path
    self.timeout = timeout
    self.parameters = parameters
    self.retry = retry
def submitNotebook(notebook):
  print("Running notebook %s" % notebook.path)
  try:
    if (notebook.parameters):
      return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
    else:
      return dbutils.notebook.run(notebook.path, notebook.timeout)
  except Exception:
    if notebook.retry < 1:
      raise
    print("Retrying notebook %s" % notebook.path)
    notebook.retry = notebook.retry - 1
    submitNotebook(notebook)
def parallelNotebooks(notebooks, numInParallel):
 # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
 # This code limits the number of parallel notebooks.
  with ThreadPoolExecutor(max_workers=numInParallel) as ec:
    return [ec.submit(submitNotebook, notebook) for notebook in notebooks]

