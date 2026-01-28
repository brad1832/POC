
import requests
def list_java_files(owner, repo, branch, java_dir, headers):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{java_dir}?ref={branch}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return [i["name"] for i in r.json() if i["type"] == "file" and i["name"].endswith(".java")]
def read_java_file(raw_base, filename):
    r = requests.get(f"{raw_base}/{filename}", timeout=20)
    r.raise_for_status()
    return r.text
