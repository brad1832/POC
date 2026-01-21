from typing import TypedDict
import requests
import base64

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END

# =====================================================
# 🔐 CONFIG
# =====================================================
GEMINI_API_KEY = ""
GITHUB_TOKEN = ""

OWNER = "brad1832"
REPO = "POC1"
BRANCH = "main"

JAVA_DIR = "src/main/java"
SCALA_DIR = "src/main/scala"
BASE_OUTPUT_FILE = "InvSalesSparkJob.scala"

JAVA_FILES = [
    "SalesMapper.java",
    "InventoryMapper.java",
    "InvSalesAggReducer.java",
    "InvSalesAggJob.java"
]

RAW_BASE = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{BRANCH}/{JAVA_DIR}"

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
# 🧠 AGENT STATE
# =====================================================
class AgentState(TypedDict):
    spark_version: str
    scala_version: str
    mr_code: str
    spark_code: str

# =====================================================
# 🛠 TOOLS (PURE, EXPLICIT INPUTS ONLY)
# =====================================================

@tool
def ask_target_versions() -> dict:
    """Ask user for Spark and Scala versions."""
    spark = input("Enter Spark version (e.g. 3.4): ").strip()
    scala = input("Enter Scala version (e.g. 2.12): ").strip()
    return {"spark_version": spark, "scala_version": scala}


@tool
def read_java_file(filename: str) -> str:
    """Read Java file from GitHub."""
    url = f"{RAW_BASE}/{filename}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.text.strip()


@tool
def convert_to_spark(
    mr_code: str,
    spark_version: str,
    scala_version: str
) -> str:
    """Convert Hadoop MapReduce Java to Spark Scala."""
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

Hadoop MapReduce Logic:
{mr_code}
""" 
    resp = llm.invoke(prompt)
    text = resp.content.strip()

    if text.startswith("```"):
        text = text.split("```")[1].strip()

    return text


@tool
def write_scala_to_github(scala_code: str) -> str:
    """Write Scala code to GitHub with auto-versioning."""

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
    versions = ask_target_versions.invoke({})
    state.update(versions)
    return state


def read_code(state: AgentState) -> AgentState:
    parts = []
    for f in JAVA_FILES:
        parts.append(read_java_file.invoke({"filename": f}))
    state["mr_code"] = "\n\n".join(parts)
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
# 🧠 BUILD AGENT GRAPH
# =====================================================
graph = StateGraph(AgentState)

graph.add_node("select_versions", select_versions)
graph.add_node("read_code", read_code)
graph.add_node("generate_spark", generate_spark)
graph.add_node("persist_output", persist_output)

graph.set_entry_point("select_versions")
graph.add_edge("select_versions", "read_code")
graph.add_edge("read_code", "generate_spark")
graph.add_edge("generate_spark", "persist_output")
graph.add_edge("persist_output", END)

agent = graph.compile()

# =====================================================
# 🚀 RUN
# =====================================================
if __name__ == "__main__":
    agent.invoke({})
