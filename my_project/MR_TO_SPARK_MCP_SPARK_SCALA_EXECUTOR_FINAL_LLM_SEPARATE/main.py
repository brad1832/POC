
from mcp_client.client import submit_job

def main():
    print("\n=== MR → Spark Scala Conversion ===\n")
    repo_url = input("Enter GitHub repository URL: ").strip()
    spark_version = input("Enter Spark version (e.g. 3.4.2): ").strip()
    scala_version = input("Enter Scala version (e.g. 2.12): ").strip()
    build_tool = input("Enter build tool (maven / gradle): ").strip().lower()

    payload = {
        "repo_url": repo_url,
        "spark_version": spark_version,
        "scala_version": scala_version,
        "build_tool": build_tool
    }

    result = submit_job(payload)
    print("\n✅ Spark project ZIP generated:", result["zip_path"])

if __name__ == "__main__":
    main()
