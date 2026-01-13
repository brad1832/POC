import os
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool
from langchain.agents import create_tool_calling_agent, AgentExecutor

# =============================
# API KEY (LOCAL TESTING)
# =============================
GEMINI_API_KEY = "AIzaSyC8BqR6YwdWYEH711Lm1FaKQKOfympOABU"

# =============================
# FILE PATHS
# =============================
INPUT_MR_PATH = r"C:\Users\BRAD\Downloads\SalesSumMapper.txt"
PROMPT_PATH   = r"C:\Users\BRAD\Downloads\Prompt.txt"
OUTPUT_PATH   = r"C:\Users\BRAD\Downloads\SparkSalesSum.scala"

# =============================
# READ FILES
# =============================
with open(INPUT_MR_PATH, "r", encoding="utf-8") as f:
    mr_code = f.read()

with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    prompt_text = f.read()

prompt = PromptTemplate(
    input_variables=["code"],
    template=prompt_text
)

# =============================
# TOOL (USES FILE PROMPT)
# =============================
@tool
def convert_mr_to_spark_scala(mr_code: str) -> str:
    """Convert MR Java code to Spark Scala"""
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0,
        google_api_key=GEMINI_API_KEY
    )

    chain = prompt | llm
    return chain.invoke({"code": mr_code}).content

# =============================
# AGENT (NO TASK PROMPT)
# =============================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

agent = create_tool_calling_agent(
    llm=llm,
    tools=[convert_mr_to_spark_scala]
)

executor = AgentExecutor(
    agent=agent,
    tools=[convert_mr_to_spark_scala],
    verbose=True
)

# =============================
# RUN AGENT
# =============================
result = executor.invoke({"input": mr_code})

spark_scala_code = result["output"]

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(spark_scala_code)

print("✅ Generated using agent WITHOUT prompt duplication")
