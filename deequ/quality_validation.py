import os

os.environ["SPARK_VERSION"] = "3.5" 
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

import pyspark
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import *
from pyspark.sql.functions import regexp_replace,when, col
from pyspark.sql.functions import col, size, split
from pyspark.sql import functions as F
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

def spark_session():
    spark = SparkSession.builder.appName("deequ").config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.9-spark-3.5") \
        .getOrCreate()
    return spark

def import_data(spark):
    # spark = spark_session()
    df = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .option("multiLine", True) \
                    .option("mode", "PERMISSIVE") \
                    .csv("data/results/locations_with_official_level_names.csv", header=True)
    return df

def shorten_hierarchy(df):
    df = spark.createDataFrame(df)

    df = df.withColumn("HIERARCHY_short", regexp_replace("HIERARCHY", r"#([^#]+)$", ""))
    
    try:
        # df.write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/processed", header=True, mode="overwrite")
        return df
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def data_validation(spark, df):
    # df = spark.createDataFrame(df)

    check = Check(spark, CheckLevel.Error, "Data Quality Checks")
    check = check \
        .isComplete("NAME") \
        .isComplete("CODE") \
        .isComplete("HIERARCHY") \
        .isComplete("LEVEL_NAME") \
        .isComplete("OFFICIAL_LEVEL_NAME") \
        .isComplete("PARENT") \
        .isComplete("LEVEL_NUMBER") \
        .isUnique("CODE") \
        .hasDistinctness(["NAME"], lambda x: x >= 0.95) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 12000) \
        .hasPattern("HIERARCHY", r"ALL#.*#.*") \
        .satisfies(
            "(LEVEL_NAME != 'COUNTRY') OR (size(split(HIERARCHY, '#')) = 4)",
            "Country-level hierarchy should have 3 #s",
            lambda x: x >= 1.0
        )\
        .satisfies(
            "array_position(split(HIERARCHY, '#'), PARENT) IS NULL OR " \
            "(size(slice(split(HIERARCHY, '#'), array_position(split(HIERARCHY, '#'), PARENT) + 1, 1000)) = LEVEL_NUMBER)",
            "LEVEL_NUMBER matches # after PARENT AND (LEVEL_NAME != 'CONTINENT')",
            lambda x: x >= 1.0
        )


    verificationResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()

    verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    try:
        verificationResult_df.coalesce(1).write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/verif_result", header=True, mode="overwrite")
        return verificationResult_df
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def detect_wrong_country_hierarchy(df):
    problematic_rows = df.filter(
        (col("LEVEL_NAME") == "COUNTRY") &
        (size(split(col("HIERARCHY"), "#")) != 4)
    )

    return problematic_rows

def get_failed_checks(check_results_df):
    failing_constraints = (
        check_results_df
        .filter((col("constraint_status") == "Failure"))
        .select("constraint")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    print("failing_constraints: ", failing_constraints)
    return failing_constraints

# Placeholder for your custom duplicate detection
def detect_duplicates(df):
    window_spec = (
        df.groupBy("NAME")
          .agg(F.count("*").alias("count"))
          .filter(F.col("count") > 1)
    )
    duplicates = df.join(window_spec, on="NAME", how="inner")
    return duplicates

def extract_problematic_rows(df, validation_result):
    problems = {}

    validation_result_pd = validation_result.toPandas()

    # UDF to calculate number of elements after parent
    def count_after_parent(hierarchy, parent):
        if hierarchy is None or parent is None:
            return None
        parts = hierarchy.split('#')
        try:
            idx = parts.index(parent)
            return len(parts[idx + 1:])
        except ValueError:
            return None

    count_after_parent_udf = F.udf(count_after_parent, T.IntegerType())

    df_with_count = df.withColumn(
        "count_after_parent",
        count_after_parent_udf(F.col("HIERARCHY"), F.col("PARENT"))
    )

    for _, row in validation_result_pd.iterrows():
        constraint = row['constraint']
        status = row['constraint_status']

        if status != 'Failure':
            continue

        if constraint.startswith("CompletenessConstraint"):
            col_name = constraint.split("Completeness(")[1].split(",")[0]
            problems[f"Null values in {col_name}"] = df.filter(F.col(col_name).isNull())

        elif constraint.startswith("DistinctnessConstraint"):
            problems["Duplicate NAME entries"] = detect_duplicates(df)

        elif constraint.startswith("PatternMatchConstraint") and "HIERARCHY" in constraint:
            problems["Incorrect HIERARCHY pattern"] = df.filter(
                ~F.col("HIERARCHY").rlike(r"^ALL#.*#.*")
            )

        elif constraint.startswith("ComplianceConstraint") and "Country-level hierarchy should have 3 #s" in constraint:
            problems["Incorrect COUNTRY level hierarchy structure"] = df.filter(
                (F.col("LEVEL_NAME") == "COUNTRY") &
                (F.size(F.split(F.col("HIERARCHY"), "#")) != 4)
            )

        elif constraint.startswith("ComplianceConstraint") and "LEVEL_NUMBER matches # after PARENT" in constraint:
            problems["LEVEL_NUMBER mismatch with HIERARCHY after PARENT"] = df_with_count.filter(
                (F.col("count_after_parent").isNotNull()) &
                (F.col("LEVEL_NUMBER") != F.col("count_after_parent")) & (F.col("LEVEL_NAME") != "CONTINENT")
            )

    return problems

if __name__ == "__main__":
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    df = import_data(spark)
    values_to_remove = ["ALL", "WORLD"]
    df = df.filter(~df["CODE"].isin(values_to_remove))
    # data_validation(spark, df)
    test = data_validation(spark, df)
    # test = spark.read.option("header", True) \
    #                .option("inferSchema", True) \
    #                 .option("quote", '"') \
    #                 .option("escape", '"') \
    #                 .option("multiLine", True) \
    #                 .option("mode", "PERMISSIVE") \
    #                 .csv("data/verif_result/part-00000-93b544e4-ae60-41b8-86a0-df9807d08e4a-c000.csv", header=True)
    # test.show()
    # Run the problem extraction
    problems = extract_problematic_rows(df, test)

    # Display or export each issue separately
    for problem_name, problem_df in problems.items():
        print(f"\n🔎 Problem detected: {problem_name}")
        problem_df.show(truncate=False)
        problem_df.coalesce(1).write.csv(f"data/problems/problem_{problem_name}.csv", header=True, mode="overwrite")

    spark.stop()
    

    
