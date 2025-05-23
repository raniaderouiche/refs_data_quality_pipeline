import os

os.environ["SPARK_VERSION"] = "3.5" 
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

import pyspark
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import *
from pyspark.sql.functions import regexp_replace,when, col
from pyspark.sql.functions import col, size, split
from pyspark.sql import functions as F
import glob
import shutil
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StringType, BooleanType, IntegerType, ArrayType

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

def data_check_warning(spark, df):
    check = Check(spark, CheckLevel.Warning, "Data Quality Checks Warning")
    check = check \
        .hasDistinctness(["NAME"], lambda x: x >= 0.96) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 11500)

def data_validation(spark, df):
    # df = spark.createDataFrame(df)

    check = Check(spark, CheckLevel.Error, "Data Quality Checks Errors")
    check = check \
        .hasDataType("NAME", ConstrainableDataTypes("String")) \
        .hasDataType("CODE", ConstrainableDataTypes("String")) \
        .hasDataType("HIERARCHY", ConstrainableDataTypes("String")) \
        .hasDataType("LEVEL_NAME", ConstrainableDataTypes("String")) \
        .hasDataType("PARENT", ConstrainableDataTypes("String")) \
        .hasDataType("OFFICIAL_LEVEL_NAME", ConstrainableDataTypes("String")) \
        .hasDataType("IS_GROUP", ConstrainableDataTypes("Boolean")) \
        .hasDataType("LEVEL_NUMBER", ConstrainableDataTypes("Numeric")) \
        .isComplete("NAME") \
        .isComplete("CODE") \
        .isComplete("HIERARCHY") \
        .isComplete("LEVEL_NAME") \
        .isComplete("OFFICIAL_LEVEL_NAME") \
        .isComplete("PARENT") \
        .isComplete("LEVEL_NUMBER") \
        .isUnique("CODE") \
        .hasDistinctness(["NAME"], lambda x: x >= 0.9) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 12000) \
        .hasPattern("HIERARCHY", r"ALL#.*#.*") \
        .satisfies(
            "(LEVEL_NAME != 'COUNTRY') OR (size(split(HIERARCHY, '#')) = 4)",
            "Country-level hierarchy should have 3 #s",
            lambda x: x >= 1.0
        )\
        .satisfies(
            "array_position(split(HIERARCHY, '#'), PARENT) IS NULL OR " \
            "(size(slice(split(HIERARCHY, '#'), array_position(split(HIERARCHY, '#'), PARENT) + 1, 1000)) = LEVEL_NUMBER) AND (LEVEL_NAME != 'CONTINENT')",
            "LEVEL_NUMBER matches # after PARENT",
            lambda x: x >= 1.0
        )\
        .satisfies(
            "false",  # placeholder
            "Missing HIERARCHY",
            lambda x: x >= 1.0
        )\
        .satisfies(
            "NOT (HIERARCHY LIKE 'ALL#WORLD#%' AND size(split(HIERARCHY, '#')) = 3) OR LEVEL_NAME = 'CONTINENT'",
            "Rows directly under WORLD should be CONTINENT level",
            lambda x: x >= 1.0
        )\
        .satisfies(
            "NOT(IS_GROUP = true AND CHILDREN IS NULL)", 
            "If IS_GROUP is true then CHILDREN must not be NULL",
            lambda assertion: assertion >= 1.0  # 100% compliance
        )\
        .satisfies(
            "NOT(CHILDREN IS NULL)",
            "If IS_GROUP is true then CHILDREN must not be empty",
            lambda assertion: assertion >= 1.0  # 100% compliance
        )

    check_warning = Check(spark, CheckLevel.Warning, "Data Quality Checks Warning")
    check_warning = check_warning \
        .hasDistinctness(["NAME"], lambda x: x >= 0.96) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 11500)

    verificationResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .addCheck(check_warning) \
        .run()

    verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    try:
        save_deequ_verif_suite(verificationResult_df)
        return verificationResult_df
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def detect_wrong_country_hierarchy(df):
    problematic_rows = df.filter(
        (col("LEVEL_NAME") == "COUNTRY") &
        (size(split(col("HIERARCHY"), "#")) != 4)
    )

    return problematic_rows

def save_deequ_verif_suite(verificationResult_df):
    verificationResult_df.coalesce(1).write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/verif_result", header=True, mode="overwrite")

    output_path = f"data/verif_result"
    file_name = "deequ_verif_result"

    # Construct the full path to the part file
    part_file_path = os.path.join(output_path, "part-00000-*")
    custom_file_name = f"{file_name}.csv"
    matching_files = glob.glob(part_file_path)

    if matching_files:
        source_file = matching_files[0]
        destination_file = os.path.join("data/results/", custom_file_name)

        # Step 1: Delete the existing file (if it exists)
        if os.path.exists(destination_file):
            try:
                if os.path.isfile(destination_file):
                    os.remove(destination_file)
            except Exception as e:
                print(f"Error deleting {destination_file}: {e}")

        os.rename(source_file, destination_file)
        print(f"Renamed '{source_file}' to '{destination_file}'") 

        # Optionally, remove the empty folder created by Spark
        try:
            shutil.rmtree(output_path)
        except OSError as e:
            print(f"Warning: Could not remove the output directory '{output_path}'. It might not be empty: {e}")
    else:
        print(f"Error: No part file found in '{output_path}'.")


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
        df.groupBy(["CODE"])
          .agg(F.count("*").alias("count"))
          .filter(F.col("count") > 1)
    )
    duplicates = df.join(window_spec, on="CODE", how="inner")
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
            problems[f"CompletenessConstraint - Null values in {col_name}"] = df.filter(F.col(col_name).isNull())

        elif constraint.startswith("AnalysisBasedConstraint(DataType("):
            col = constraint.split("DataType(")[1].split(",")[0]
            if col == "LEVEL_NUMBER":
                problems[f"AnalysisBasedConstraint - {col} has incorrect datatype"] = df.filter(
                    ~df[col].cast("int").isNotNull()
                )
            elif col == "IS_GROUP":
                problems[f"AnalysisBasedConstraint - {col} has incorrect datatype"] = df.filter(
                    ~df[col].cast("boolean").isNotNull()
                )
            elif col == "CHILDREN":
                problems[f"AnalysisBasedConstraint - {col} has incorrect datatype"] = df.filter(
                    df[col].isNotNull() & ~df[col].cast("array<string>").isNotNull()
                )
            else:
                problems[f"AnalysisBasedConstraint - {col} has incorrect datatype"] = df.filter(
                    ~df[col].cast("string").isNotNull()
                )

        # elif constraint.startswith("DistinctnessConstraint"):
        #     problems["Duplicate NAME entries"] = detect_duplicates(df)

        elif constraint.startswith("UniquenessConstraint"):
            problems["UniquenessConstraint - Duplicate CODE entries"] = detect_duplicates(df)

        elif constraint.startswith("PatternMatchConstraint") and "HIERARCHY" in constraint:
            problems["PatternMatchConstraint - Incorrect HIERARCHY pattern"] = df.filter(
                ~F.col("HIERARCHY").rlike(r"^ALL#.*#.*")
            )

        elif constraint.startswith("ComplianceConstraint") and "Country-level hierarchy should have 3 #s" in constraint:
            problems["ComplianceConstraint - Incorrect COUNTRY level hierarchy structure"] = df.filter(
                (F.col("LEVEL_NAME") == "COUNTRY") &
                (F.size(F.split(F.col("HIERARCHY"), "#")) != 4)
            )

        elif constraint.startswith("ComplianceConstraint") and "LEVEL_NUMBER matches # after PARENT" in constraint:
            problems["ConsistencyConstraint - LEVEL_NUMBER mismatch with HIERARCHY after PARENT"] = df_with_count.filter(
                (F.col("count_after_parent").isNotNull()) &
                (F.col("LEVEL_NUMBER") != F.col("count_after_parent")) & (F.col("LEVEL_NAME") != "CONTINENT")
            )

        elif constraint.startswith("ComplianceConstraint") and "Missing HIERARCHY" in constraint:
            df_with_parent = df.withColumn(
                "parent_hierarchy",
                F.expr("regexp_replace(HIERARCHY, '#[^#]+$', '')")
            ).filter(F.col("LEVEL_NUMBER") >= 0)

            df_parents = df.select(F.col("HIERARCHY").alias("existing_hierarchy"))

            problems["ConsistencyConstraint - Orphan hierarchies (parent missing)"] = df_with_parent.join(
                df_parents,
                df_with_parent["parent_hierarchy"] == df_parents["existing_hierarchy"],
                how="left_anti"
            ).drop("parent_hierarchy")

        elif constraint.startswith("ComplianceConstraint") and "Rows directly under WORLD should be CONTINENT level" in constraint:
            problems["ComplianceConstraint - Non-CONTINENT rows directly under WORLD"] = df.filter(
                (F.col("HIERARCHY").startswith("ALL#WORLD#")) &
                (F.size(F.split(F.col("HIERARCHY"), "#")) == 3) &
                (F.col("LEVEL_NAME") != "CONTINENT")
            )
            
        elif constraint.startswith("ComplianceConstraint") and ("If IS_GROUP is true then CHILDREN must not be NULL") in constraint:
            problems["ConsistencyConstraint - CHILDREN NULL"] = df.filter(
                (F.col("IS_GROUP") == True) & (
                    F.col("CHILDREN").isNull() |
                    (F.trim(F.col("CHILDREN")) == "") |
                    (F.col("CHILDREN") == "[]")
                )
            )
        elif constraint.startswith("ComplianceConstraint") and ("If IS_GROUP is true then CHILDREN must not be empty") in constraint:
            problems["ConsistencyConstraint - CHILDREN NULL"] = df.filter(
                (F.col("IS_GROUP") == True) & (
                    F.col("CHILDREN").isNull() |
                    (F.trim(F.col("CHILDREN")) == "") |
                    (F.col("CHILDREN") == "[]")
                )
            )


    return problems

def deequ_quality_check(spark, df):
    df = spark.createDataFrame(df)
    values_to_remove = ["ALL", "WORLD"]
    df = df.filter(~df["CODE"].isin(values_to_remove))
    # data_validation(spark, df)
    test = data_validation(spark, df)
    problems = extract_problematic_rows(df, test)

    # Display or export each issue separately
    for problem_name, problem_df in problems.items():
        print(f"\n🔎 Problem detected: {problem_name}")
        problem_df.show(truncate=False)
        # problem_df = problem_df.withColumn("problem_name", F.lit(problem_name))
        problem_df = problem_df.select("NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME", "PARENT", "LEVEL_NUMBER", "OFFICIAL_LEVEL_NAME")
        problem_df.coalesce(1).write.csv(f"data/problems/problem_{problem_name}", header=True, mode="overwrite")
        output_path = f"data/problems/problem_{problem_name}"
        # Construct the full path to the part file
        part_file_path = os.path.join(output_path, "part-00000-*")
        custom_file_name = f"{problem_name}.csv"
        matching_files = glob.glob(part_file_path)

        if matching_files:
            source_file = matching_files[0]
            destination_file = os.path.join("data/problems/", custom_file_name)

            # Check if the destination file already exists
            if os.path.exists(destination_file):
                try:
                    os.remove(destination_file)
                    print(f"Existing file '{destination_file}' overwritten.")
                except OSError as e:
                    print(f"Error: Could not delete existing file '{destination_file}': {e}")
                    # You might want to handle this error more gracefully, e.g., skip renaming
                    exit(1)  # Or some other error handling
            try:
                os.rename(source_file, destination_file)
                print(f"Renamed '{source_file}' to '{destination_file}'")
            except OSError as e:
                print(f"Error: Could not rename file '{source_file}' to '{destination_file}': {e}")


            # Optionally, remove the empty folder created by Spark
            try:
                shutil.rmtree(output_path)
            except OSError as e:
                print(f"Warning: Could not remove the output directory '{output_path}'. It might not be empty: {e}")
        else:
            print(f"Error: No part file found in '{output_path}'.")
    
    return problems

def import_data_from_path(spark, path):
    # spark = spark_session()
    df = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .option("multiLine", True) \
                    .option("mode", "PERMISSIVE") \
                    .csv(path, header=True)
    return df

if __name__ == "__main__":
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    df = import_data(spark)

    values_to_remove = ["ALL", "WORLD"]
    df = df.filter(~df["CODE"].isin(values_to_remove))
    # data_validation(spark, df)
    test = data_validation(spark, df)
    problems = extract_problematic_rows(df, test)
    
    # Display or export each issue separately
    # for problem_name, problem_df in problems.items():
    #     print(f"\n🔎 Problem detected: {problem_name}")
    #     problem_df.show(truncate=False)
    #     shutil.rmtree("data/problems")
    #     os.makedirs("data/problems", exist_ok=True)
    #     # problem_df = problem_df.withColumn("problem_name", F.lit(problem_name))
    #     problem_df = problem_df.select("NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME")
    #     problem_df.coalesce(1).write.csv(f"data/problems/problem_{problem_name}", header=True, mode="overwrite")
    #     output_path = f"data/problems/problem_{problem_name}"
    #     # Construct the full path to the part file
    #     part_file_path = os.path.join(output_path, "part-00000-*")
    #     custom_file_name = f"{problem_name}.csv"
    #     matching_files = glob.glob(part_file_path)

    #     if matching_files:
    #         source_file = matching_files[0]
    #         destination_file = os.path.join("data/problems/", custom_file_name)

    #         # Check if the destination file already exists
    #         if os.path.exists(destination_file):
    #             try:
    #                 os.remove(destination_file)
    #                 print(f"Existing file '{destination_file}' overwritten.")
    #             except OSError as e:
    #                 print(f"Error: Could not delete existing file '{destination_file}': {e}")
    #                 # You might want to handle this error more gracefully, e.g., skip renaming
    #                 exit(1)  # Or some other error handling
    #         try:
    #             os.rename(source_file, destination_file)
    #             print(f"Renamed '{source_file}' to '{destination_file}'")
    #         except OSError as e:
    #             print(f"Error: Could not rename file '{source_file}' to '{destination_file}': {e}")


    #         # Optionally, remove the empty folder created by Spark
    #         try:
    #             shutil.rmtree(output_path)
    #         except OSError as e:
    #             print(f"Warning: Could not remove the output directory '{output_path}'. It might not be empty: {e}")
    #     else:
    #         print(f"Error: No part file found in '{output_path}'.")

    # Display or export each issue separately
    for problem_name, problem_df in problems.items():
        print(f"\n🔎 Problem detected: {problem_name}")
        problem_df.show(truncate=False)
        # problem_df = problem_df.withColumn("problem_name", F.lit(problem_name))
        problem_df = problem_df.select("NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME", "PARENT", "LEVEL_NUMBER", "OFFICIAL_LEVEL_NAME")
        problem_df.coalesce(1).write.csv(f"data/problems/problem_{problem_name}", header=True, mode="overwrite")
        output_path = f"data/problems/problem_{problem_name}"
        # Construct the full path to the part file
        part_file_path = os.path.join(output_path, "part-00000-*")
        custom_file_name = f"{problem_name}.csv"
        matching_files = glob.glob(part_file_path)

        if matching_files:
            source_file = matching_files[0]
            destination_file = os.path.join("data/problems/", custom_file_name)

            # Check if the destination file already exists
            if os.path.exists(destination_file):
                try:
                    os.remove(destination_file)
                    print(f"Existing file '{destination_file}' overwritten.")
                except OSError as e:
                    print(f"Error: Could not delete existing file '{destination_file}': {e}")
                    # You might want to handle this error more gracefully, e.g., skip renaming
                    exit(1)  # Or some other error handling
            try:
                os.rename(source_file, destination_file)
                print(f"Renamed '{source_file}' to '{destination_file}'")
            except OSError as e:
                print(f"Error: Could not rename file '{source_file}' to '{destination_file}': {e}")


            # Optionally, remove the empty folder created by Spark
            try:
                shutil.rmtree(output_path)
            except OSError as e:
                print(f"Warning: Could not remove the output directory '{output_path}'. It might not be empty: {e}")
        else:
            print(f"Error: No part file found in '{output_path}'.")

    spark.stop()
    

    
