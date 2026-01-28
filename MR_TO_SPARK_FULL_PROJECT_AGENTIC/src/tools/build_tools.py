
def generate_build(build_tool, spark_version, scala_version):
    if build_tool == "maven":
        return "pom.xml", f"<project><properties><spark.version>{spark_version}</spark.version></properties></project>"
    return "build.gradle", f"dependencies {{ implementation 'org.apache.spark:spark-sql_{scala_version}:{spark_version}' }}"
