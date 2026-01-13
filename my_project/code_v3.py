from pathlib import Path
import requests
from langchain_google_genai import ChatGoogleGenerativeAI

# =====================================================
# 🔐 CONFIG
# =====================================================
GEMINI_API_KEY = "AIzaSyC8BqR6YwdWYEH711Lm1FaKQKOfympOABU "

# GitHub raw base for your repo
# Repo: https://github.com/brad1832/POC1
GITHUB_RAW_BASE = (
    "https://raw.githubusercontent.com/brad1832/POC1/main/src/main/java"
)

# Java files in repo
MAPPER_FILES = [
    "SalesMapper.java",
    "InventoryMapper.java"
]

REDUCER_FILE = "InvSalesAggReducer.java"
DRIVER_FILE  = "InvSalesAggJob.java"

# Local output file (ON YOUR MACHINE)
OUTPUT_PATH = Path(r"C:\Users\BRAD\Downloads\InvSalesSparkJob.scala")

# =====================================================
# 🤖 LLM
# =====================================================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

# =====================================================
# 📖 HELPER: READ FILE FROM GITHUB
# =====================================================
def read_from_github(filename: str) -> str:
    url = f"{GITHUB_RAW_BASE}/{filename}"
    response = requests.get(url)
    response.raise_for_status()
    return response.text

# =====================================================
# 🚀 PIPELINE
# =====================================================
def run_pipeline():
    parts = []

    # Read mappers
    for idx, file in enumerate(MAPPER_FILES, start=1):
        code = read_from_github(file)
        parts.append(f"// Mapper {idx}\n{code}")

    # Read reducer
    parts.append(f"// Reducer\n{read_from_github(REDUCER_FILE)}")

    # Read driver
    parts.append(f"// Driver\n{read_from_github(DRIVER_FILE)}")

    # Combine MR code
    mr_code = "\n\n".join(parts)

    # Prompt Gemini
    prompt = f"""
You are a senior data engineer.

Convert the following Hadoop MapReduce Java code into Spark Scala.

Rules:
- Use Spark DataFrame API
- Produce ONE runnable Spark job
- Preserve business logic
- Output ONLY Scala code (no explanation)

MapReduce Code:
{mr_code}
"""

    response = llm.invoke(prompt)
    spark_scala_code = response.content

    # Write output locally
    OUTPUT_PATH.write_text(spark_scala_code, encoding="utf-8")

    print("✅ Conversion successful")
    print(f"📄 Spark Scala output written to:\n{OUTPUT_PATH}")

# =====================================================
# 🟢 ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run_pipeline()
