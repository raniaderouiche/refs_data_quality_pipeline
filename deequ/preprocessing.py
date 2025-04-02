import os
os.environ["SPARK_VERSION"] = "3.5" 

import pyspark
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import *
from pyspark.sql.functions import regexp_replace,when, col

def spark_session():
    spark = SparkSession.builder.appName("deequ").config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.9-spark-3.5") \
        .getOrCreate()
    return spark

def import_data(spark):
    # spark = spark_session()
    df = spark.read.csv("C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/DNEXR.REFERENCE.location.csv", header=True)
    return df

# def check_data(spark,df):
#     result = VerificationSuite(spark).onData(df).addCheck(
#         Check(spark, CheckLevel.Error, "Data Quality Check")
#         .isComplete("NAME")
#     ).run()

#     result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
#     result_df.show()
#     if result_df.filter("check_status != 'Success'").count() > 0:
#         raise Exception("Data quality checks failed")

def data_exploration(spark,df):
    if not isinstance(df, pyspark.sql.DataFrame):
        df = spark.createDataFrame(df)
    analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Completeness("NAME")) \
                    .addAnalyzer(Completeness("LEVEL_NAME")) \
                    .addAnalyzer(Distinctness("NAME")) \
                    .addAnalyzer(Completeness("HIERARCHY")) \
                    .addAnalyzer(PatternMatch("HIERARCHY",r"^ALL#WORLD#.*")) \
                    .addAnalyzer(ApproxCountDistinct("NAME")) \
                    .run()
                    
    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    analysisResult_df.show()
    analysisResult_df.coalesce(1).write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/analysis_result", header=True, mode="overwrite")


def shorten_hierarchy(df):
    df = spark.createDataFrame(df)

    df = df.withColumn("HIERARCHY_short", regexp_replace("HIERARCHY", r"#([^#]+)$", ""))
    
    try:
        # df.write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/processed", header=True, mode="overwrite")
        return df
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def data_checks(spark, df):
    df = spark.createDataFrame(df)

    check = Check(spark, CheckLevel.Error, "Data Quality Checks")
    check = check \
        .isComplete("NAME") \
        .isComplete("LEVEL_NAME") \
        .isComplete("HIERARCHY") \
        .hasDistinctness(["NAME"], lambda x: x >= 0.85) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 10000) \
        .hasPattern("HIERARCHY", r"^ALL#WORLD#([A-Z]+#)*[A-Z]+$", lambda x: x >= 0.95)


    verificationResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()

    verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    try:
        verificationResult_df.coalesce(1).write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/verif_result", header=True, mode="overwrite")
        spark.stop()
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def preprocess_level_names(spark, df):
    df = spark.createDataFrame(df)
    # Define allowed values
    allowed_values = {"COUNTRY", "CITY", "PORT_TERMINAL"}
    
    # Apply transformation using when and otherwise
    df = df.withColumn("LEVEL_NAME", when(col("LEVEL_NAME").isin(allowed_values), col("LEVEL_NAME")).otherwise("REGION"))
    
    # Return a sample of 100 rows
    sample_df = df.limit(1500)
    return sample_df.toPandas()


if __name__ == "__main__":
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark, df = import_data()
    data_checks(spark, df)
    print(df)
    
