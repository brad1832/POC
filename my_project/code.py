# from openai import OpenAI
#
# client = OpenAI(
#     api_key="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"
# )
#
# response = client.chat.completions.create(
#     model="gpt-4o-mini",
#     messages=[
#         {"role": "user", "content": "Explain Python lists simply"}
#     ]
# )
#
# print(response.choices[0].message.content)
# import google.genai as genai
#
# # Hard-code API key (OK for testing)
# genai.configure(api_key="AIzaSyC8BqR6YwdWYEH711Lm1FaKQKOfympOABU")
#
# # Use Gemini Flash 2.5
# model = genai.GenerativeModel("gemini-2.5-flash")
#
# response = model.generate_content(
#     "Explain Python lists in simple words"
# )
#
# print(response.text)
import os
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate

# =====================================================
# 🔐 GEMINI API KEY (FOR LOCAL TESTING ONLY)
# =====================================================
GEMINI_API_KEY = "AIzaSyC8BqR6YwdWYEH711Lm1FaKQKOfympOABU"

# =====================================================
# 📂 FILE PATHS (WINDOWS ABSOLUTE PATHS)
# =====================================================
INPUT_MR_PATH = r"C:\Users\BRAD\Downloads\SalesSumMapper.txt"
PROMPT_PATH   = r"C:\Users\BRAD\Downloads\Prompt.txt"
OUTPUT_PATH   = r"C:\Users\BRAD\Downloads\SparkSalesSum.scala"

# =====================================================
# 📖 READ MAPREDUCE JAVA CODE
# =====================================================
with open(INPUT_MR_PATH, "r", encoding="utf-8") as f:
    mr_code = f.read()

# =====================================================
# 📖 READ PROMPT FROM FILE
# =====================================================
with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    prompt_text = f.read()

prompt = PromptTemplate(
    input_variables=["code"],
    template=prompt_text
)

# =====================================================
# 🤖 INITIALIZE GEMINI FLASH 2.5
# =====================================================
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=GEMINI_API_KEY
)

# =====================================================
# 🔗 LANGCHAIN RUNNABLE PIPELINE
# =====================================================
chain = prompt | llm

# =====================================================
# 🚀 INVOKE LLM
# =====================================================
response = chain.invoke({"code": mr_code})
spark_scala_code = response.content

# =====================================================
# 📁 ENSURE OUTPUT DIRECTORY EXISTS
# =====================================================
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# =====================================================
# 💾 WRITE OUTPUT SPARK SCALA CODE
# =====================================================
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(spark_scala_code)

print("✅ Spark Scala code generated successfully!")
print(f"📄 Output file: {OUTPUT_PATH}")
