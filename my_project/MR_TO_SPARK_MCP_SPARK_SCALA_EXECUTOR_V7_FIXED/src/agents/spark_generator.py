from src.llm.llm_client import get_llm
from src.prompts.spark_builder_prompt import SPARK_BUILDER_PROMPT
from src.agents.mr_analyzer import analyze_mr

llm = get_llm()

def generate_spark(state: dict) -> dict:
    # ---- HARD SAFETY FIX ----
    # Ensure semantic_model ALWAYS exists

    if "semantic_model" not in state:
        state = analyze_mr(state)

    # If still missing (analyzer failed), force it
    if "semantic_model" not in state:
        state["semantic_model"] = state.get("mr_code", "")

    semantic_model = state["semantic_model"]

    prompt = SPARK_BUILDER_PROMPT.format(
        semantic_model=semantic_model
    )

    resp = llm.invoke(prompt)
    state["spark_code"] = resp.content.strip()
    return state
