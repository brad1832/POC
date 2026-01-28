SPARK_BUILDER_SYSTEM_PROMPT = """
You are a senior Spark Scala engineer.
Convert analyzed MapReduce logic into Spark Scala.
Output runnable Spark code only.
"""

SPARK_BUILDER_HUMAN_PROMPT = """
Spark Version: {spark_version}
Scala Version: {scala_version}
Build Tool: {build_tool}

Mapper Logic:
{mapper_logic}

Reducer Logic:
{reducer_logic}

Driver Logic:
{driver_logic}

Assumptions:
{assumptions}
"""
