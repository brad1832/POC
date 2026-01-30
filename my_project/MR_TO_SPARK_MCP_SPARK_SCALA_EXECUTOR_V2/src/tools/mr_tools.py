
def detect_mr_role(code: str, filename: str) -> str:
    c = code.lower()
    f = filename.lower()
    if "mapper" in c or "mapper" in f:
        return "MAPPER"
    if "reducer" in c or "reducer" in f:
        return "REDUCER"
    return "DRIVER"
