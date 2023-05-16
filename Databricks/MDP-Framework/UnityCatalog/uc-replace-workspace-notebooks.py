# Databricks notebook source
# MAGIC %run ../Common/common-workspace

# COMMAND ----------

def WorkspaceExport(path="/"):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/workspace/export"
    data = json.dumps({ "path": path , "format": "SOURCE" })
    response = requests.get(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def WorkspaceImport(path, language, content):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/workspace/import"
    data = json.dumps( { 
        "path": path
        ,"content": content
        ,"language": "PYTHON" if language.upper() == "PY" else language.upper()
        ,"overwrite": "true"
        ,"format": "SOURCE" })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def WorkspaceMkDir(path):
    headers = GetAuthenticationHeader()
    url = f"{INSTANCE_NAME}/api/2.0/workspace/mkdirs"
    data = json.dumps( { "path": path } )
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def NotebookReplaceText(path):
    import base64, re
    newPath = "/".join( path.split("/")[0:-1] + path.split("/")[-1:] ) + "-old"
    notebook = WorkspaceExport(path)
    if "content" not in str(notebook):
        return

    fileType = notebook["file_type"]
    base64content = notebook['content']
    c = base64.b64decode(base64content).decode("utf-8", "ignore")

    if not(re.search('curated_v2', c, re.IGNORECASE)):
        return
    print(f"Replacing: {path}")
    
    # BACKUP ORIGINAL
    WorkspaceImport(newPath, fileType, base64content)

    # REPLACE TEXT
    c = re.compile(re.escape("curated_v2"), re.IGNORECASE).sub("curated", c)
    c = base64.b64encode(bytes(c, "ascii")).decode("utf-8")

    # IMPORT NEW REPLACEMENT
    WorkspaceImport(path, fileType, c)

# COMMAND ----------

def Identify(path):
    import base64, re
    newPath = "/".join( path.split("/")[0:-1] + path.split("/")[-1:] ) + "-old"
    notebook = WorkspaceExport(path)
    if "content" not in str(notebook):
        return
    base64content = notebook['content']
    c = base64.b64decode(base64content).decode("utf-8", "ignore")

    if re.search('curated_v2', c, re.IGNORECASE):
        print(path)

# COMMAND ----------

def WorkspaceMkDirTarget(path):
    headers = GetAuthenticationHeader("daf-target-databricks-token")
    url = f"{INSTANCE_NAME}/api/2.0/workspace/mkdirs"
    data = json.dumps( { "path": path } )
    response = requests.post(url, headers=headers, data=data)
    jsonResponse = response.json()

# COMMAND ----------

def WorkspaceImportTarget(path, language, content):
    headers = GetAuthenticationHeader("daf-target-databricks-token")
    url = f"{INSTANCE_NAME}/api/2.0/workspace/import"
    data = json.dumps( { 
        "path": path
        ,"content": content
        ,"language": "PYTHON" if language.upper() == "PY" else language.upper()
        ,"overwrite": "true"
        ,"format": "SOURCE" })
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# COMMAND ----------

def MigrateNotebook(path):
    j = WorkspaceExport(path)
    print(WorkspaceImportTarget(path, j["file_type"], j["content"]))

# COMMAND ----------

def RecursiveIteratePath(path, fileMethod, directoryMethod=None):
    if "backup" in path.lower() or "Jar" in path.lower() or "trash" in path.lower() or "/users/o" in path.lower() :
        return

    pathListing = ListWorkspaces(path)

    if "objects" not in str(pathListing):
        return

    for p in (pathListing.selectExpr("explode(objects) o")
        .select("o.*", "o.path").collect()):
        print(p.path)
        if (p.object_type != "DIRECTORY"):
            fileMethod(p.path)

        if (p.object_type == "DIRECTORY"):
            directoryMethod(p.path) if directoryMethod is not None else ()
            RecursiveIteratePath(p.path, fileMethod, directoryMethod)
RecursiveIteratePath("/Users/3bj@sydneywater.com.au", Identify)
#RecursiveIteratePath("/Users")

# COMMAND ----------


