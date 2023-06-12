import requests, os, json, base64

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

def upload_zip_to_workspace(url:str, pat_token:str, env:str, user_tli:str):
    """push given zip file to end user's workspace folder"""
    with open(f'.//user_notebooks//{env}//zips//{user_tli}.zip','rb') as zip_data:
        url += "api/2.0/workspace/import"
        payload={'path': f'/Users/{user_tli}@sydneywater.com.au/ucmigration_backup'}

        files=[
        ('content',('backup.zip', zip_data,'application/zip'))
        ]

        headers = {
        'Authorization': f'Bearer {pat_token}'
        }
        print (f"Attempting to push file {user_tli}.zip.")
        response = requests.request("POST", url, headers=headers, data=payload, files=files, timeout=5000)
        print (f"{user_tli}.zip has been successfully uploaded to /Users/{user_tli}@sydneywater.com.au/ucmigration_backup.") if response.status_code == 200 else print (response.text)


def send_backup_zips(url:str, pat_token:str, env:str):
    for file in os.listdir(os.getcwd() + f'//user_notebooks//{env}//zips'):
        if 'zip' in file and 'o9ji' in file: #comment out the second part here before migration day usage
            user_tli = file.split('.')[0]
            upload_zip_to_workspace(url, pat_token, env, user_tli)


def upload_notebooks(url:str, pat_token:str, user_tli:str, filename:str, content:str, subfolder:str=''):
    """takes base64 encoded content and uploads the content to a notebook in the user folder. if existing already, the notebook is overwritten."""
    url += "api/2.0/workspace/import"
    language = "PYTHON" if '.py' in filename else 'SQL' if '.sql' in filename else ''
    path = f"/Users/{user_tli}@sydneywater.com.au/{subfolder}/{filename.split('.')[0]}"
    payload = {
    "path": path,
    "content": content,
    "language": language,
    "overwrite": True,
    "format": "SOURCE"
    }

    json_payload = json.dumps(payload)
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {pat_token}'
    }

    response = requests.request("POST", url, headers=headers, data=json_payload)
    print(f"File `{filename}` successfully uploaded to {path}") if response.status_code == 200 else print(response.text)

def post_file_contents(url:str, pat_token:str, user_folder:str, path:str, subfolder:str='') -> None:
    filename = path.split('\\')[-1]
    print (filename)
    with open (path, 'rb') as notebook_text:
        encoded_string = base64.b64encode(notebook_text.read())
        upload_notebooks(url, pat_token, user_folder, filename, encoded_string.decode('utf8'), subfolder)

def send_updated_notebooks(url:str, pat_token:str, env:str):
    user_notebooks_path = os.getcwd() + f'\\user_notebooks\\{env}\\notebooks'
    for user_folder in os.listdir(user_notebooks_path):
        if 'zip' not in user_folder and 'o9ji' in user_folder: #comment out the second part here before migration day usage
            for content in os.listdir(user_notebooks_path + f'\\{user_folder}\\{user_folder}@sydneywater.com.au'):
                if os.path.isfile(os.path.join(user_notebooks_path, user_folder, f'{user_folder}@sydneywater.com.au', content)):
                    print (f'Attempting to push file `{content}`')
                    post_file_contents(url, pat_token, user_folder, (os.path.join(user_notebooks_path, user_folder, f'{user_folder}@sydneywater.com.au', content)) )
                else:
                    if 'Trash' not in content:
                        for sub_content in os.listdir(os.path.join(user_notebooks_path, user_folder, f'{user_folder}@sydneywater.com.au', content)):
                            post_file_contents(url, pat_token, user_folder, (os.path.join(os.getcwd(), 'user_notebooks', env, 'notebooks', user_folder, f'{user_folder}@sydneywater.com.au', content, sub_content)), content )


if __name__ == '__main__':
    # upload_zip_to_workspace(dev_url, dev_pat_token, 'dev', 'o9ji')
    # send_backup_zips(dev_url, dev_pat_token, 'dev')
    # upload_notebooks(dev_url, dev_pat_token, 'o9ji', 'test.py', 'Ly8gRGF0YWJyaWNrcyBub3RlYm9vayBzb3VyY2UKMSsx')
    send_updated_notebooks(dev_url, dev_pat_token, 'dev')