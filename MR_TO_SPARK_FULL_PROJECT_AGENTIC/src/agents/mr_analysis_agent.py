
from src.tools.github_tools import list_java_files, read_java_file
from src.tools.mr_tools import detect_mr_role

def mr_analysis_agent(state):
    files = list_java_files(state["owner"], state["repo"], state["branch"], state["java_dir"], state["headers"])
    parts = []
    for f in files:
        code = read_java_file(state["raw_base"], f)
        role = detect_mr_role(code, f)
        parts.append(f"[{role}] {f}\n{code}")
    state["mr_code"] = "\n\n".join(parts)
    return state
