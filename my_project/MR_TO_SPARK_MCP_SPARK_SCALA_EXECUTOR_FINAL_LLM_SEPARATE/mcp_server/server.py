
from src.agents.mr_analyzer import analyze_mr
from src.agents.spark_generator import generate_spark
from src.agents.spark_project_assembler import assemble_project

def handle_request(state: dict) -> dict:
    state = analyze_mr(state)
    state = generate_spark(state)
    state = assemble_project(state)
    return state
