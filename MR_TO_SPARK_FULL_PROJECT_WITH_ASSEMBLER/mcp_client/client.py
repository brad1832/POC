from mcp_server.server import handle_request

def submit_job(repo_url: str, build_tool: str, spark_version: str, scala_version: str):
    payload = {
        "repo_url": repo_url,
        "build_tool": build_tool,
        "spark_version": spark_version,
        "scala_version": scala_version
    }
    return handle_request(payload)
