import requests, os, json, base64

preprod_url = "https://adb-XXXXXXXXX.azuredatabricks.net/"
prod_url = "https://adb-XXXXXXXXX.azuredatabricks.net/"
dev_url = "https://adb-XXXXXXXXX.azuredatabricks.net/"
test_url = "https://adb-XXXXXXXXX.azuredatabricks.net/"
dev_pat_token = os.getenv("dev_pat_token")
test_pat_token = os.getenv("test_pat_token")
preprod_pat_token = os.getenv("preprod_pat_token")
prod_pat_token = os.getenv("prod_pat_token")
path = "/Users"

def upload_zip_to_workspace(url:str, pat_token:str, env:str, user_tli:str):
    """push given zip file to end user's workspace folder"""
    with open(f'.//user_notebooks//{env}//zips//{user_tli}.zip','rb') as zip_data:
        url += "api/2.0/workspace/import"
        payload={'path': f'/Users/{user_tli}@companyx.com.au/ucmigration_backup'}

        files=[
        ('content',('backup.zip', zip_data,'application/zip'))
        ]

        headers = {
        'Authorization': f'Bearer {pat_token}'
        }
        print (f"Attempting to push file {user_tli}.zip.")
        response = requests.request("POST", url, headers=headers, data=payload, files=files, timeout=5000)
        print (f"{user_tli}.zip has been successfully uploaded to /Users/{user_tli}@companyx.com.au/ucmigration_backup.") if response.status_code == 200 else print (response.text)


def send_backup_zips(url:str, pat_token:str, env:str):
    """iterate through all backups and push them to user's workspace folder """
    for file in os.listdir(os.getcwd() + f'//user_notebooks//{env}//zips'):
        if 'zip' in file:
            user_tli = file.split('.')[0]
            upload_zip_to_workspace(url, pat_token, env, user_tli)


def upload_notebooks(url:str, pat_token:str, user_tli:str, filename:str, content:str, subfolder:str=''):
    """takes base64 encoded content and uploads the content to a notebook in the user folder. if existing already, the notebook is overwritten."""
    url += "api/2.0/workspace/import"
    language = "PYTHON" if '.py' in filename else 'SQL' if '.sql' in filename else ''
    path = f"/Users/{user_tli}@companyx.com.au/{subfolder}/{filename.split('.')[0]}"
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
    """opens given file and posts the file to a path in databricks"""
    filename = path.split('\\')[-1]
    print (filename)
    with open (path, 'rb') as notebook_text:
        encoded_string = base64.b64encode(notebook_text.read())
        upload_notebooks(url, pat_token, user_folder, filename, encoded_string.decode('utf8'), subfolder)

def send_updated_notebooks(url:str, pat_token:str, env:str):
    user_notebooks_path = os.getcwd() + f'\\user_notebooks\\{env}\\notebooks'
    for user_folder in os.listdir(user_notebooks_path):
        if 'zip' not in user_folder:
            for content in os.listdir(user_notebooks_path + f'\\{user_folder}\\{user_folder}@companyx.com.au'):
                if os.path.isfile(os.path.join(user_notebooks_path, user_folder, f'{user_folder}@companyx.com.au', content)):
                    print (f'Attempting to push file `{content}`')
                    post_file_contents(url, pat_token, user_folder, (os.path.join(user_notebooks_path, user_folder, f'{user_folder}@companyx.com.au', content)) )
                else:
                    if 'Trash' not in content:
                        for sub_content in os.listdir(os.path.join(user_notebooks_path, user_folder, f'{user_folder}@companyx.com.au', content)):
                            post_file_contents(url, pat_token, user_folder, (os.path.join(os.getcwd(), 'user_notebooks', env, 'notebooks', user_folder, f'{user_folder}@companyx.com.au', content, sub_content)), content )


if __name__ == '__main__':
    send_updated_notebooks(dev_url, dev_pat_token, 'dev')