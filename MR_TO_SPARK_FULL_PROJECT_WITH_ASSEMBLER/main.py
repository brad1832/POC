from mcp_client.client import submit_job

def main():
    repo = input("GitHub Repo URL: ")
    build = input("Build Tool (maven/gradle): ")
    spark = input("Spark Version: ")
    scala = input("Scala Version: ")

    result = submit_job(repo, build, spark, scala)
    print(result["spark_code"])

if __name__ == "__main__":
    main()
