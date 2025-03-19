import os
os.environ["SPARK_VERSION"] = "3.5" 

from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import *
from pyspark.sql.functions import regexp_replace


spark = SparkSession.builder.appName("deequ").config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.9-spark-3.5") \
    .getOrCreate()

def import_data():
    df = spark.read.csv("C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/DNEXR.REFERENCE.location.csv", header=True)
    return df

def check_data(df):
    result = VerificationSuite(spark).onData(df).addCheck(
        Check(spark, CheckLevel.Error, "Data Quality Check")
        .isComplete("NAME")
    ).run()

    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show()
    if result_df.filter("check_status != 'Success'").count() > 0:
        raise Exception("Data quality checks failed")

def data_exploration(df):
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

def preprocess_df(df):
    df = df.withColumn("HIERARCHY_short", regexp_replace("HIERARCHY", r"#([^#]+)$", ""))
    
    try:
        df.write.csv("file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/processed", header=True, mode="overwrite")
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def data_checks(df):
    check = Check(spark, CheckLevel.Error, "Data Quality Checks")
    check = check \
        .isComplete("NAME") \
        .isComplete("LEVEL_NAME") \
        .isComplete("HIERARCHY") \
        .hasDistinctness(["NAME"], lambda x: x >= 0.85) \
        .hasApproxCountDistinct("NAME", lambda x: x >= 10000) \
        .hasPattern("HIERARCHY", r"your_regex_here", lambda x: x >= 0.95)


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

if __name__ == "__main__":
    spark.sparkContext.setLogLevel("ERROR")
    df = import_data()
    preprocess_df(df)
    data_checks(df)
    
