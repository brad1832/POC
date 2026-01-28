
def detect_mr_role(java_code: str, filename: str) -> str:
    code = java_code.lower()
    name = filename.lower()
    if "extends mapper" in code: return "MAPPER"
    if "extends reducer" in code: return "REDUCER"
    if "job.getinstance" in code: return "DRIVER"
    if "mapper" in name: return "MAPPER"
    if "reducer" in name: return "REDUCER"
    if "driver" in name or "job" in name: return "DRIVER"
    return "UNKNOWN"
