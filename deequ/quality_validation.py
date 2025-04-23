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
import re

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
        .satisfies(
                "CODE IN ('ALL', 'WORLD') OR PARENT IS NOT NULL",
                "PARENT completeness excluding ALL/WORLD",
                lambda x: x >= 1.0
            ) \
        .satisfies(
                "CODE IN ('ALL', 'WORLD') OR LEVEL_NUMBER IS NOT NULL",
                "LEVEL_NUMBER completeness excluding ALL/WORLD",
                lambda x: x >= 1.0
            ) \
        .isComplete("OFFICIAL_LEVEL_NAME") \
        .isUnique("CODE") \
        .hasDistinctness(["NAME"], lambda x: x >= 0.95) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 12000) \
        .hasPattern("HIERARCHY", r"ALL#.*#.*")\
        .satisfies(
            "(LEVEL_NAME != 'COUNTRY') OR (size(split(HIERARCHY, '#')) = 4)",
            "Country-level hierarchy should have 3 #s",
            lambda x: x >= 1.0  # Pass threshold: all rows matching
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

def get_completeness_violations_df(check_results_df, data_df):
    failing_constraints = (
        check_results_df
        .filter((col("constraint_status") == "Failure"))
        .select("constraint")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    filters = []
    print("failing_constraints: ", failing_constraints)
    for constraint in failing_constraints:
        # Handle CompletenessConstraint
        match = re.search(r"CompletenessConstraint\(Completeness\((\w+)", constraint)
        if match:
            column = match.group(1)
            filters.append((column, None))  # None = no exclusion

        # Handle PARENT compliance condition
        elif "Compliance(PARENT completeness excluding ALL/WORLD" in constraint:
            filters.append(("PARENT", ["ALL", "WORLD"]))

        # Handle PARENT compliance condition
        elif "Compliance(LEVEL_NUMBER completeness excluding ALL/WORLD" in constraint:
            filters.append(("LEVEL_NUMBER", ["ALL", "WORLD"]))

    failing_rows = []

    for column, exclusions in filters:
        if exclusions:
            filtered = data_df.filter(
                col(column).isNull() & (~col("CODE").isin(exclusions))
            )
        else:
            filtered = data_df.filter(col(column).isNull())

        failing_rows.append(filtered)

    if not failing_rows:
        return data_df.limit(0)
    elif len(failing_rows) == 1:
        return failing_rows[0]
    else:
        return failing_rows[0].unionByName(*failing_rows[1:])

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

if __name__ == "__main__":
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    df = import_data(spark)
    # data_validation(spark, df)
    # test = data_validation(spark, df)
    test = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .option("multiLine", True) \
                    .option("mode", "PERMISSIVE") \
                    .csv("data/verif_result/part-00000-93b544e4-ae60-41b8-86a0-df9807d08e4a-c000.csv", header=True)
    test.show()
    problems = get_failed_checks(test)

    spark.stop()
    

    
