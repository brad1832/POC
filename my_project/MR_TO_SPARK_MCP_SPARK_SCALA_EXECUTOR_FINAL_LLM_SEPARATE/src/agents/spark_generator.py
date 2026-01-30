
import os
from src.llm.llm_client import get_llm

SYSTEM_PROMPT = open("prompts/system_prompt.txt").read()
HUMAN_PROMPT = open("prompts/human_prompt.txt").read()

llm = get_llm()

def generate_spark(state: dict) -> dict:
    prompt = SYSTEM_PROMPT + "\n\n" + HUMAN_PROMPT.format(
        spark_version=state["spark_version"],
        scala_version=state["scala_version"],
        build_tool=state["build_tool"],
        semantic_model=state.get("semantic_model", state.get("mr_code",""))
    )
    resp = llm.invoke(prompt)
    state["spark_code"] = resp.content.strip()
    return state
