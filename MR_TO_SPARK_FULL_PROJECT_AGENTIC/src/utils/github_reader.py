import requests

def read_repo(repo_url: str) -> dict:
    api_url = repo_url.replace("github.com", "api.github.com/repos") + "/contents"
    r = requests.get(api_url)
    r.raise_for_status()

    files = {}
    for item in r.json():
        if item["name"].endswith(".java"):
            files[item["name"]] = requests.get(item["download_url"]).text
    return files
