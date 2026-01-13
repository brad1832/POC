from pathlib import Path
import requests
import base64
from langchain_google_genai import ChatGoogleGenerativeAI

# =====================================================
# 🔐 CONFIG
# =====================================================
GEMINI_API_KEY = "AIzaSyC8BqR6YwdWYEH711Lm1FaKQKOfympOABU"
GITHUB_TOKEN = "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"

OWNER = "brad1832"
REPO = "POC1"
BRANCH = "main"

GITHUB_RAW_BASE = (
    f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{BRANCH}/src/main/java"
)

# Where Spark output will be stored IN GITHUB
GITHUB_OUTPUT_DIR = "src/main/scala"
BASE_OUTPUT_FILE = "InvSalesSparkJob.scala"

MAPPER_FILES = [
    "SalesMapper.java",
    "InventoryMapper.java"
]

REDUCER_FILE = "InvSalesAggReducer.java"
DRIVER_FILE = "InvSalesAggJob.java"

# =====================================================
# 🤖 LLM
# =====================================================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# =====================================================
# 📖 READ FILE FROM GITHUB
# =====================================================
def read_from_github(filename: str) -> str:
    url = f"{GITHUB_RAW_BASE}/{filename}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.text.strip()

# =====================================================
# 📦 CHECK FILE EXISTS IN GITHUB
# =====================================================
def github_file_exists(path: str) -> bool:
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{path}?ref={BRANCH}"
    return requests.get(url, headers=HEADERS).status_code == 200

# =====================================================
# 🧮 FIND NEXT AVAILABLE FILENAME
# =====================================================
def get_next_filename() -> str:
    if not github_file_exists(f"{GITHUB_OUTPUT_DIR}/{BASE_OUTPUT_FILE}"):
        return BASE_OUTPUT_FILE

    i = 1
    while True:
        candidate = BASE_OUTPUT_FILE.replace(".scala", f"_{i}.scala")
        if not github_file_exists(f"{GITHUB_OUTPUT_DIR}/{candidate}"):
            return candidate
        i += 1

# =====================================================
# ⬆️ WRITE FILE TO GITHUB
# =====================================================
def write_to_github(filename: str, content: str):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{GITHUB_OUTPUT_DIR}/{filename}"

    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": f"Add Spark job {filename}",
        "content": encoded,
        "branch": BRANCH
    }

    r = requests.put(url, headers=HEADERS, json=payload, timeout=20)
    r.raise_for_status()

# =====================================================
# 🚀 PIPELINE
# =====================================================
def run_pipeline():
    parts = []

    for idx, file in enumerate(MAPPER_FILES, start=1):
        parts.append(f"// Mapper {idx}\n{read_from_github(file)}")

    parts.append(f"// Reducer\n{read_from_github(REDUCER_FILE)}")
    parts.append(f"// Driver\n{read_from_github(DRIVER_FILE)}")

    mr_code = "\n\n".join(parts)

    prompt = f"""
Convert the following Hadoop MapReduce Java code into Spark Scala.

Rules:
- Use Spark DataFrame API
- Produce ONE runnable Spark job
- Include SparkSession
- No explanation
- Output ONLY Scala code

Code:
{mr_code}
"""

    response = llm.invoke(prompt)
    scala_code = response.content.strip()

    if scala_code.startswith("```"):
        scala_code = scala_code.split("```")[1].strip()

    output_file = get_next_filename()
    write_to_github(output_file, scala_code)

    print("✅ Conversion successful")
    print(f"📄 Spark Scala written to GitHub:")
    print(f"   {GITHUB_OUTPUT_DIR}/{output_file}")

# =====================================================
# 🟢 ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run_pipeline()
