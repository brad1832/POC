from src.llm.gemini_chat_model import get_llm
from src.prompts.mr_analysis_prompt import (
    MR_ANALYSIS_SYSTEM_PROMPT,
    MR_ANALYSIS_HUMAN_PROMPT
)
from src.utils.github_reader import read_repo

def analyze_mr(state: dict) -> dict:
    llm = get_llm()
    java_files = read_repo(state["repo_url"])

    combined = []
    for name, code in java_files.items():
        combined.append(f"FILE: {name}\n{code}")

    prompt = MR_ANALYSIS_SYSTEM_PROMPT + MR_ANALYSIS_HUMAN_PROMPT.format(
        mr_code="\n\n".join(combined)
    )

    response = llm.invoke(prompt).content
    return {"analysis": response}
