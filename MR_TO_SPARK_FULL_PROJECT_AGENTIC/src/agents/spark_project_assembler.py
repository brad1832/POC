
import os
import zipfile
import tempfile

def assemble_spark_project(scala_code: str, build_tool: str, spark_version: str, scala_version: str) -> str:
    """
    Assemble a full runnable Spark Scala project as a ZIP.
    """
    with tempfile.TemporaryDirectory() as tmp:
        # Spark directory structure
        scala_dir = os.path.join(tmp, "src", "main", "scala")
        os.makedirs(scala_dir, exist_ok=True)

        # Write Spark Scala job
        job_path = os.path.join(scala_dir, "SparkJob.scala")
        with open(job_path, "w", encoding="utf-8") as f:
            f.write(scala_code)

        # Build files
        if build_tool == "maven":
            build_file = f"""
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>spark</groupId>
  <artifactId>spark-job</artifactId>
  <version>1.0</version>
  <properties>
    <scala.version>{scala_version}</scala.version>
    <spark.version>{spark_version}</spark.version>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_{scala_version}</artifactId>
      <version>{spark_version}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
"""
            name = "pom.xml"
        else:
            build_file = f"""
plugins {{
    id 'scala'
}}
dependencies {{
    implementation "org.apache.spark:spark-sql_{scala_version}:{spark_version}"
}}
"""
            name = "build.gradle"

        with open(os.path.join(tmp, name), "w", encoding="utf-8") as f:
            f.write(build_file)

        # Zip project
        out_zip = "spark_scala_project.zip"
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(tmp):
                for file in files:
                    full = os.path.join(root, file)
                    z.write(full, arcname=full.replace(tmp + os.sep, ""))

        return out_zip
