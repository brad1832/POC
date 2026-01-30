
import os
from src.tools.github_tools import parse_repo, list_java_files, read_java_file
from src.tools.mr_tools import detect_mr_role

def analyze_mr(state: dict) -> dict:
    owner, repo, branch = parse_repo(state["repo_url"])
    token = os.environ["GITHUB_TOKEN"]

    files = list_java_files(owner, repo, branch, token)
    parts = []
    for f in files:
        code = read_java_file(owner, repo, branch, f)
        role = detect_mr_role(code, f)
        parts.append(f"[{role}] {f}\n{code}")

    state["mr_code"] = "\n\n".join(parts)
    return state
