from src.llm.gemini_chat_model import get_llm
from src.prompts.spark_builder_prompt import (
    SPARK_BUILDER_SYSTEM_PROMPT,
    SPARK_BUILDER_HUMAN_PROMPT
)

def build_spark(state: dict) -> dict:
    llm = get_llm()

    prompt = SPARK_BUILDER_SYSTEM_PROMPT + SPARK_BUILDER_HUMAN_PROMPT.format(
        spark_version=state["spark_version"],
        scala_version=state["scala_version"],
        build_tool=state["build_tool"],
        mapper_logic=state["analysis"],
        reducer_logic=state["analysis"],
        driver_logic=state["analysis"],
        assumptions="Refer to analysis"
    )

    spark_code = llm.invoke(prompt).content
    return {"spark_code": spark_code}
