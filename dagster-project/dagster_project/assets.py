
from dagster import AssetIn, asset, AssetKey, AssetExecutionContext, resource
import sys
import os
os.environ["SPARK_VERSION"] = "3.5" 
os.environ["PYSPARK_PYTHON"] = "C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/venv/Scripts/python.exe"
from dagster_dbt import dbt_assets,DbtCliResource
from .project import ref_dq
from dagster import AssetExecutionContext
from pyspark.sql import SparkSession


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from deequ.preprocessing import import_data, preprocess_level_names,data_checks,shorten_hierarchy,data_exploration
from scripts.anomalies_detection import detect_anomalies
from scripts.distilBERT_model import bert
from scripts.duplicates_detection_by_name import name_duplicates_detection
from scripts.static_duplicate_detection import run_static_detection
from scripts.detection_comparison import comparison
@resource
def spark_session():
    spark = SparkSession.builder.appName("deequ").config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.9-spark-3.5") \
        .getOrCreate()
    yield spark
    spark.stop()

@asset(description="Loads data from a CSV file into a DataFrame.",
       required_resource_keys={"spark_session"})
def import_data_asset(context):
    spark_session = context.resources.spark_session
    df = import_data(spark_session)
    return df.toPandas()

@asset(description="Explores the data using Deequ's analysis tools.",
       required_resource_keys={"spark_session"})
def deequ_data_exploration(context,import_data_asset):
    df = import_data_asset
    spark_session = context.resources.spark_session
    data_exploration(spark_session, df)

@asset(description="Checks the data quality using Deequ's checks on predefined tests.",
       required_resource_keys={"spark_session"})
def deequ_data_checks(context,import_data_asset):
    df = import_data_asset
    spark_session = context.resources.spark_session
    data_checks(spark_session, df)

@asset(description="Redefines the level names",
       required_resource_keys={"spark_session"})
def preprocess_level_names_asset(context,import_data_asset):
    df = import_data_asset
    spark_session = context.resources.spark_session
    return preprocess_level_names(spark_session,df)

@asset(description="Searching for duplicates in the data using fuzzy wuzzy library.")
def static_detection_asset(preprocess_level_names_asset):
    return run_static_detection(preprocess_level_names_asset)

@asset(description="Searching for duplicates in the name column.")
def name_duplicates_detection_asset(preprocess_level_names_asset):
    return name_duplicates_detection(preprocess_level_names_asset)

@asset(description="Applying the distilBERT model the data.")
def bert_model(name_duplicates_detection_asset):
    return bert(name_duplicates_detection_asset)

@asset(description="Comparing the two methods")
def detection_comparison_asset(bert_model, static_detection_asset):
    return comparison(bert_model,static_detection_asset)

@asset(description="Detecting anomalies using distilBERT.")
def detect_anomalies(preprocess_level_names_asset):
    detect_anomalies(preprocess_level_names_asset)

# -- dbt assets --
@dbt_assets(manifest=ref_dq.manifest_path)
def ref_dq_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# @asset(ins={"dbt_output": AssetIn(key="locations")})
# def run_spark_job(dbt_output):
#     # Assuming dbt_output is a list of tuples representing rows of data
#     df = import_data(dbt_output)
#     check_data(df)