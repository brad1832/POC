from src.agents.mr_analyzer import analyze_mr
from src.agents.spark_generator import generate_spark
from src.agents.spark_project_assembler import assemble_project

def handle_request(state: dict) -> dict:
    # Step 1: MR -> semantic model
    state = analyze_mr(state)

    # Step 2: semantic model -> Spark Scala
    state = generate_spark(state)

    # Step 3: assemble Spark project + build
    state = assemble_project(state)

    return state