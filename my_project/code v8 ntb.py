from typing import TypedDict, List
import os
import requests
import base64

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END

# =====================================================
# 🔐 CONFIG
# =====================================================

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY not set")

if not GITHUB_TOKEN:
    raise RuntimeError("GITHUB_TOKEN not set")

OWNER = "brad1832"
REPO = "POC1"
BRANCH = "main"

JAVA_DIR = "src/main/java"
SCALA_DIR = "src/main/scala"
BASE_OUTPUT_FILE = "InvSalesSparkJob.scala"

RAW_BASE = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{BRANCH}/{JAVA_DIR}"

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
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
# 🧠 SHARED STATE
# =====================================================

class AgentState(TypedDict):
    spark_version: str
    scala_version: str
    mr_code: str
    spark_code: str

# =====================================================
# 🔍 MAPREDUCE ROLE DETECTION
# =====================================================

def detect_mr_role(java_code: str, filename: str) -> str:
    code = java_code.lower()
    name = filename.lower()

    if "extends mapper" in code or "implements mapper" in code:
        return "MAPPER"

    if "extends reducer" in code or "implements reducer" in code:
        return "REDUCER"

    if (
        "job.getinstance" in code
        or "setmapperclass" in code
        or "setreducerclass" in code
    ):
        return "DRIVER"

    if "mapper" in name:
        return "MAPPER"
    if "reducer" in name:
        return "REDUCER"
    if "driver" in name or "job" in name:
        return "DRIVER"

    return "UNKNOWN"

# =====================================================
# 🛠 TOOLS
# =====================================================

@tool
def ask_target_versions() -> dict:
    """Ask user for Spark and Scala versions."""
    spark = input("Enter Spark version (e.g. 3.4): ").strip()
    scala = input("Enter Scala version (e.g. 2.12): ").strip()
    return {"spark_version": spark, "scala_version": scala}


@tool
def list_java_files_from_github() -> List[str]:
    """List all Java files from the given GitHub directory."""
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{JAVA_DIR}?ref={BRANCH}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()

    items = r.json()

    return [
        item["name"]
        for item in items
        if item["type"] == "file" and item["name"].endswith(".java")
    ]


@tool
def read_java_file(filename: str) -> str:
    """Read Java source file from GitHub."""
    url = f"{RAW_BASE}/{filename}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.text


@tool
def convert_to_spark(
    mr_code: str,
    spark_version: str,
    scala_version: str
) -> str:
    """Convert parsed MapReduce logic into Spark Scala."""
    prompt = f"""
You are a senior Spark developer.

Convert the following parsed Hadoop MapReduce logic into Spark Scala code.

Rules:
- Use Spark DataFrame API (Scala)
- Create a runnable object with main method
- Preserve mapper and reducer semantics
- Add minimal comments
- Do NOT explain outside the code

Target environment:
- Apache Spark {spark_version}
- Scala {scala_version}

Input is structured as:
- [MAPPER] files define transformation logic
- [REDUCER] files define aggregation logic
- [DRIVER] files define job orchestration

Parsed Hadoop MapReduce Logic:
{mr_code}
"""
    resp = llm.invoke(prompt)
    text = resp.content.strip()

    if text.startswith("```"):
        text = text.split("```")[1].strip()

    return text


@tool
def write_scala_to_github(scala_code: str) -> str:
    """Write Spark Scala code to GitHub with auto-versioning."""

    def exists(path: str) -> bool:
        url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{path}?ref={BRANCH}"
        return requests.get(url, headers=HEADERS).status_code == 200

    filename = BASE_OUTPUT_FILE
    i = 1
    while exists(f"{SCALA_DIR}/{filename}"):
        filename = BASE_OUTPUT_FILE.replace(".scala", f"_{i}.scala")
        i += 1

    encoded = base64.b64encode(scala_code.encode()).decode()
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{SCALA_DIR}/{filename}"

    payload = {
        "message": f"Add Spark job {filename}",
        "content": encoded,
        "branch": BRANCH
    }

    r = requests.put(url, headers=HEADERS, json=payload, timeout=20)
    r.raise_for_status()

    return f"{SCALA_DIR}/{filename}"

# =====================================================
# 🧠 LANGGRAPH NODES
# =====================================================

def select_versions(state: AgentState) -> AgentState:
    state.update(ask_target_versions.invoke({}))
    return state


def read_and_classify_code(state: AgentState) -> AgentState:
    java_files = list_java_files_from_github.invoke({})

    if not java_files:
        raise RuntimeError("No Java files found in the GitHub directory")

    structured_parts = []

    for filename in java_files:
        java_code = read_java_file.invoke({"filename": filename})
        role = detect_mr_role(java_code, filename)

        structured_parts.append(
            f"[{role}] {filename}\n{java_code}"
        )

    state["mr_code"] = "\n\n".join(structured_parts)
    return state


def generate_spark(state: AgentState) -> AgentState:
    state["spark_code"] = convert_to_spark.invoke({
        "mr_code": state["mr_code"],
        "spark_version": state["spark_version"],
        "scala_version": state["scala_version"]
    })
    return state


def persist_output(state: AgentState) -> AgentState:
    path = write_scala_to_github.invoke({"scala_code": state["spark_code"]})
    print(f"\n✅ Spark Scala job written to GitHub: {path}")
    return state

# =====================================================
# 🧠 BUILD GRAPH
# =====================================================

graph = StateGraph(AgentState)

graph.add_node("select_versions", select_versions)
graph.add_node("read_and_classify_code", read_and_classify_code)
graph.add_node("generate_spark", generate_spark)
graph.add_node("persist_output", persist_output)

graph.set_entry_point("select_versions")
graph.add_edge("select_versions", "read_and_classify_code")
graph.add_edge("read_and_classify_code", "generate_spark")
graph.add_edge("generate_spark", "persist_output")
graph.add_edge("persist_output", END)

agent = graph.compile()

# =====================================================
# 🚀 RUN
# =====================================================

if __name__ == "__main__":
    agent.invoke({})
