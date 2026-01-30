import os, zipfile, tempfile

def assemble_project(state: dict) -> dict:
    build_tool = state.get("build_tool")

    with tempfile.TemporaryDirectory() as tmp:
        scala_dir = os.path.join(tmp, "src/main/scala")
        os.makedirs(scala_dir, exist_ok=True)

        # Write Spark Scala code
        with open(os.path.join(scala_dir, "SparkJob.scala"), "w") as f:
            f.write(state["spark_code"])

        # Maven build template
        pom = f"""<project xmlns="http://maven.apache.org/POM/4.0.0"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
http://maven.apache.org/xsd/maven-4.0.0.xsd">
<modelVersion>4.0.0</modelVersion>
<groupId>org.migration</groupId>
<artifactId>mr-to-spark</artifactId>
<version>1.0.0</version>
<properties>
<scala.version>{state.get("scala_version","2.12.18")}</scala.version>
<spark.version>{state.get("spark_version","3.5.0")}</spark.version>
</properties>
<dependencies>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-core_2.12</artifactId>
<version>${{spark.version}}</version>
<scope>provided</scope>
</dependency>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-sql_2.12</artifactId>
<version>${{spark.version}}</version>
<scope>provided</scope>
</dependency>
</dependencies>
</project>"""

        # Gradle build template
        gradle = f"""plugins {{
    id 'scala'
    id 'application'
}}

group = 'org.migration'
version = '1.0.0'

repositories {{ mavenCentral() }}

dependencies {{
    implementation 'org.scala-lang:scala-library:{state.get("scala_version","2.12.18")}'
    implementation 'org.apache.spark:spark-core_2.12:{state.get("spark_version","3.5.0")}'
    implementation 'org.apache.spark:spark-sql_2.12:{state.get("spark_version","3.5.0")}'
}}

application {{
    mainClass = 'SparkJob'
}}
"""

        # ---- ONLY generate selected build ----
        if build_tool == "maven":
            with open(os.path.join(tmp, "pom.xml"), "w") as f:
                f.write(pom)

        elif build_tool == "gradle":
            with open(os.path.join(tmp, "build.gradle"), "w") as f:
                f.write(gradle)

        else:
            raise ValueError("build_tool must be 'maven' or 'gradle'")

        # zip output
        zip_path = "spark_project.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            for root, _, files in os.walk(tmp):
                for file in files:
                    full = os.path.join(root, file)
                    z.write(full, arcname=full.replace(tmp + os.sep, ""))

        state["zip_path"] = zip_path
        return state