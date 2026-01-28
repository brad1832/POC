from src.agents.mr_graph import run_graph

def handle_request(payload: dict) -> dict:
    """MCP Server entrypoint"""
    return run_graph(payload)
