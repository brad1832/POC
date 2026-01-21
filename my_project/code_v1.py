from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool
from langchain.agents import create_tool_calling_agent, AgentExecutor

# =====================================================
# 🔐 API KEY (LOCAL TESTING ONLY)
# =====================================================
GEMINI_API_KEY = "===="

# =====================================================
# 🤖 LLM
# =====================================================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

# =====================================================
# 🛠️ TOOL: CONVERT PARSED MR → SPARK SCALA
# =====================================================
@tool
def convert_mr_to_spark_scala(parsed_mr_logic: str) -> str:
    """
    Converts parsed MapReduce logic into Spark Scala code.
    """
    prompt = PromptTemplate.from_template("""
    You are a senior Spark developer.

    Convert the following parsed Hadoop MapReduce logic into Spark Scala code.

    Rules:
    - Use Spark DataFrame API (Scala)
    - Create a runnable object with main method
    - Preserve mapper and reducer semantics
    - Add minimal comments
    - Do NOT explain outside the code

    Parsed MapReduce Logic:
    {logic}

    Generate Spark Scala code:
    """)

    chain = prompt | llm
    return chain.invoke({"logic": parsed_mr_logic}).content


# =====================================================
# 🤖 AGENT FOR CONVERSION
# =====================================================
agent_prompt = PromptTemplate.from_template("""
You are a MapReduce to Spark conversion agent.

Your task:
- Take parsed MapReduce logic
- Convert it into Spark Scala code using available tools
""")

agent = create_tool_calling_agent(
    llm=llm,
    tools=[convert_mr_to_spark_scala],
    prompt=agent_prompt
)

executor = AgentExecutor(
    agent=agent,
    tools=[convert_mr_to_spark_scala],
    verbose=True
)

# =====================================================
# 📦 INPUT FROM PREVIOUS STEP (SIMULATED)
# =====================================================
# This would normally come from the parser agent output
parsed_mr_logic = """
{
  "mapper": {
    "input_key": "LongWritable",
    "input_value": "Text",
    "output_key": "Text",
    "output_value": "IntWritable",
    "logic": "Split input line by comma and emit (product, amount)"
  },
  "reducer": {
    "input_key": "Text",
    "input_value": "Iterable<IntWritable>",
    "output_key": "Text",
    "output_value": "IntWritable",
    "logic": "Sum all amounts for each product"
  },
  "driver": {
    "input_format": "TextInputFormat",
    "output_format": "TextOutputFormat"
  }
}
"""

# =====================================================
# 🚀 RUN CONVERSION
# =====================================================
result = executor.invoke({"input": parsed_mr_logic})

spark_scala_code = result["output"]

print("\n===== GENERATED SPARK SCALA CODE =====\n")
print(spark_scala_code)
