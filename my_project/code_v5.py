from pathlib import Path
import requests
import base64
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import tool

# =====================================================
# 🔐 CONFIG
# =====================================================
GEMINI_API_KEY = "----"
GITHUB_TOKEN = "----"

OWNER = "brad1832"
REPO = "POC1"
BRANCH = "main"

GITHUB_RAW_BASE = (
    f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{BRANCH}/src/main/java"
)

GITHUB_OUTPUT_DIR = "src/main/scala"
BASE_OUTPUT_FILE = "InvSalesSparkJob.scala"

MAPPER_FILES = [
    "SalesMapper.java",
    "InventoryMapper.java"
]

REDUCER_FILE = "InvSalesAggReducer.java"
DRIVER_FILE = "InvSalesAggJob.java"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# =====================================================
# 🤖 LLM
# =====================================================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

# =====================================================
# 🛠 TOOLS
# =====================================================

@tool
def read_java_from_github(filename: str) -> str:
    """Read a Java source file from GitHub."""
    url = f"{GITHUB_RAW_BASE}/{filename}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.text.strip()


@tool
def convert_mr_to_spark(mr_code: str) -> str:
    """Convert Hadoop MapReduce Java code into runnable Spark Scala."""
    prompt = f"""
Convert Hadoop MapReduce Java code into Spark Scala.

Rules:
- Use Spark DataFrame API
- Include SparkSession
- Produce ONE runnable Spark job
- Output ONLY Scala code

Code:
{mr_code}
"""
    resp = llm.invoke(prompt)
    text = resp.content.strip()

    if text.startswith("```"):
        text = text.split("```")[1].strip()

    return text


@tool
def write_scala_to_github(scala_code: str) -> str:
    """Write Scala Spark job to GitHub with auto-versioning."""
    def exists(path: str) -> bool:
        url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{path}?ref={BRANCH}"
        return requests.get(url, headers=HEADERS).status_code == 200

    if not exists(f"{GITHUB_OUTPUT_DIR}/{BASE_OUTPUT_FILE}"):
        filename = BASE_OUTPUT_FILE
    else:
        i = 1
        while True:
            candidate = BASE_OUTPUT_FILE.replace(".scala", f"_{i}.scala")
            if not exists(f"{GITHUB_OUTPUT_DIR}/{candidate}"):
                filename = candidate
                break
            i += 1

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{GITHUB_OUTPUT_DIR}/{filename}"
    encoded = base64.b64encode(scala_code.encode()).decode()

    payload = {
        "message": f"Add Spark job {filename}",
        "content": encoded,
        "branch": BRANCH
    }

    r = requests.put(url, headers=HEADERS, json=payload, timeout=20)
    r.raise_for_status()

    return f"{GITHUB_OUTPUT_DIR}/{filename}"

# =====================================================
# 🚀 PIPELINE (TOOLS USED EXPLICITLY)
# =====================================================
def run_pipeline():
    parts = []

    for file in MAPPER_FILES:
        parts.append(read_java_from_github.invoke({"filename": file}))

    parts.append(read_java_from_github.invoke({"filename": REDUCER_FILE}))
    parts.append(read_java_from_github.invoke({"filename": DRIVER_FILE}))

    mr_code = "\n\n".join(parts)

    spark_code = convert_mr_to_spark.invoke({"mr_code": mr_code})
    github_path = write_scala_to_github.invoke({"scala_code": spark_code})

    print("✅ Conversion successful")
    print(f"📄 Spark Scala written to GitHub: {github_path}")


# =====================================================
# 🟢 ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run_pipeline()
