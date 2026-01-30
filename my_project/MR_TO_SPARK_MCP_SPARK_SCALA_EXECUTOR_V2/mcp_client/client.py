
from mcp_server.server import handle_request

def submit_job(payload: dict) -> dict:
    return handle_request(payload)
