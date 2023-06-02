import requests, os, json, re, zipfile

hackathon_url = "https://adb-2536357900957616.16.azuredatabricks.net/"
preprod_url = "https://adb-2717485385283200.0.azuredatabricks.net/"
prod_url = "https://adb-5148661409353183.3.azuredatabricks.net/"
dev_url = "https://adb-7004525605760210.10.azuredatabricks.net/"
test_url = "https://adb-1108007146617792.12.azuredatabricks.net/"
hackathon_pat_token = os.getenv("hackathon_pat_token")
dev_pat_token = os.getenv("dev_pat_token")
test_pat_token = os.getenv("test_pat_token")
preprod_pat_token = os.getenv("preprod_pat_token")
prod_pat_token = os.getenv("prod_pat_token")
path = "/Users"


def list_all_workspace_folders(url:str, pat_token:str, path) -> list:
  """get all folders from the given workspace."""
  url += "api/2.0/workspace/list"
  headers = {
  'Accept': 'application/json',
  'Authorization': f'Bearer {pat_token}',
  'Content-Type': 'application/json'
  }
  payload = json.dumps({
    "path": f"{path}"
  })
  response = requests.request("GET", url, headers=headers, data=payload, timeout=20000)
  json_response = json.loads(response.text)
  try:
    objects = json_response['objects']
    object_path_list = [object['path'] for object in objects]
    return object_path_list
  except Exception as e:
    return []


def get_valid_workspace_paths(url:str, pat_token:str, path:str):
  """filter out the list of workspace folders that have only 1 item (Trash)."""
  complete_list = list_all_workspace_folders(url, pat_token, path)
  new_list = []
  for object in complete_list:
        #discard folders that only have Trash folder
        if len(list_all_workspace_folders(url, pat_token, object)) > 1: 
              new_list.append(object)
        else:
              pass
  print (new_list)
  return new_list


      
def download_workspace_folder(url:str, pat_token:str, folder_path:str, download_path:str=None):
    """download contents of given folder path into user_notebooks folder as zip file and extract them."""
    url += "api/2.0/workspace/export"
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {pat_token}',
    'Content-Type': 'application/json'
    }
    payload = json.dumps({
      "path": f"{folder_path}",
      "format": "SOURCE",
      "direct_download": True
    })
    
    user = re.search(r"/([a-zA-Z0-9]+)@", folder_path).group(1) if re.search(r"/([a-zA-Z0-9]+)@", folder_path) else 'folderx'

    response = requests.request("GET", url, headers=headers, data=payload, timeout=1000)
    with open(f'./user_notebooks/{download_path}/zips/{user}.zip', 'wb') as data:
      data.write(response.content)

    with zipfile.ZipFile(f'./user_notebooks/{download_path}/zips/{user}.zip', 'r') as zip_ref:
      zip_ref.extractall(f'./user_notebooks/{download_path}/notebooks/{user}/')


def get_workspace_folders(url, pat_token, download_path):
    for user_path in get_valid_workspace_paths(url, pat_token, path="/Users"):
      download_workspace_folder(url, pat_token, user_path, download_path)



if __name__ == '__main__':
      # print (list_all_workspace_folders(dev_url, dev_pat_token, path))
      # print (get_valid_workspace_paths(dev_url, dev_pat_token, path))
      # download_workspace_folder(url, headers, "/Users/o9ji@sydneywater.com.au")
  # for user_path in get_valid_workspace_paths(hackathon_url, hackathon_pat_token, path):
  #   download_workspace_folder(hackathon_url, hackathon_pat_token, user_path, download_path="hackathon")
  # get_workspace_folders(hackathon_url, hackathon_pat_token, "/hackathon")
  get_workspace_folders(dev_url, dev_pat_token, "/dev")
  # get_workspace_folders(prod_url, prod_pat_token, "/prod")




#if directing the API call to a single file, you can print out the response (and also save the file as a .py file)
# print(response.text)
