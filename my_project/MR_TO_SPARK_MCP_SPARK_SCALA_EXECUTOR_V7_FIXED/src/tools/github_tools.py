
import requests

def parse_repo(url: str):
    parts = url.replace("https://github.com/", "").split("/")
    return parts[0], parts[1], "main"

def list_java_files(owner, repo, branch, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/src/main/java"
    headers = {"Authorization": f"token {token}"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return [f["name"] for f in r.json() if f["name"].endswith(".java")]

def read_java_file(owner, repo, branch, filename):
    raw = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/src/main/java/{filename}"
    return requests.get(raw).text
