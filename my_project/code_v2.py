from pathlib import Path
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import tool

# =====================================================
# 🔐 CONFIG
# =====================================================
GEMINI_API_KEY = "AIzaSyC8BqR6YwdWYEH711Lm1FaKQKOfympOABU "

MAPPER_PATHS = [
    r"C:\Users\BRAD\Downloads\SalesMapper.java",
    r"C:\Users\BRAD\Downloads\InventoryMapper.java",
]

REDUCER_PATH = r"C:\Users\BRAD\Downloads\InvSalesAggReducer.java"
DRIVER_PATH  = r"C:\Users\BRAD\Downloads\InvSalesAggJob.java"

OUTPUT_PATH  = Path(r"C:\Users\BRAD\Downloads\InvSalesSparkJob.scala")

# =====================================================
# 🤖 LLM
# =====================================================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

# =====================================================
# 🛠️ TOOL: READ ALL MR FILES
# =====================================================
@tool
def read_mr_files() -> str:
    """
    Read Mapper, Reducer, and Driver Java files and
    return a single combined MapReduce payload.
    """
    parts = []

    for i, path in enumerate(MAPPER_PATHS, start=1):
        parts.append(f"// Mapper {i}\n{Path(path).read_text(encoding='utf-8')}")

    parts.append(f"// Reducer\n{Path(REDUCER_PATH).read_text(encoding='utf-8')}")
    parts.append(f"// Driver\n{Path(DRIVER_PATH).read_text(encoding='utf-8')}")

    return "\n\n".join(parts)

# =====================================================
# 🛠️ TOOL: CONVERT TO SPARK
# =====================================================
@tool
def convert_to_spark(mr_code: str) -> str:
    """
    Convert Hadoop MapReduce Java code into Spark Scala code.
    """
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
    return llm.invoke(prompt).content

# =====================================================
# 🚀 PIPELINE (DETERMINISTIC)
# =====================================================
def run_pipeline():
    # Step 1: Read input files (tool)
    mr_code = read_mr_files.invoke({})

    # Step 2: Convert to Spark (tool)
    spark_code = convert_to_spark.invoke({"mr_code": mr_code})

    # Step 3: Write output
    OUTPUT_PATH.write_text(spark_code, encoding="utf-8")

    print(f"✅ Spark Scala code written to: {OUTPUT_PATH}")

# =====================================================
# 🟢 ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run_pipeline()
