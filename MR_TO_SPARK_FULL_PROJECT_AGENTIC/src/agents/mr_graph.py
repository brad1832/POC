from src.agents.mr_analysis_agent import analyze_mr
from src.agents.spark_builder_agent import build_spark

def run_graph(state: dict) -> dict:
    analysis = analyze_mr(state)
    spark = build_spark({**state, **analysis})
    return spark


# ---------------- NEW FINAL STEP ----------------
from src.agents.spark_project_assembler import assemble_spark_project

def generate_executable_spark_project(state: dict) -> dict:
    """
    Final step: package Spark Scala code into runnable project ZIP
    """
    zip_path = assemble_spark_project(
        scala_code=state["spark_code"],
        build_tool=state.get("build_tool", "maven"),
        spark_version=state["spark_version"],
        scala_version=state["scala_version"]
    )
    state["spark_project_zip"] = zip_path
    return state
