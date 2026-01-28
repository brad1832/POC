MR_ANALYSIS_SYSTEM_PROMPT = """
You are a senior Hadoop MapReduce migration architect.
Analyze code and extract mapper, reducer, driver logic.
Do NOT generate Spark code.
Return structured logic only.
"""

MR_ANALYSIS_HUMAN_PROMPT = """
Analyze the following MapReduce code:

{mr_code}
"""
