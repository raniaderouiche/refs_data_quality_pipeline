
from dagster import AssetIn, asset, AssetKey, AssetExecutionContext
import sys
import os

from dagster_dbt import dbt_assets,DbtCliResource
from .project import ref_dq
from dagster import AssetExecutionContext


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from deequ.preprocessing import import_data, check_data

@asset
def run_spark_job():
    import_data()

@dbt_assets(manifest=ref_dq.manifest_path)
def ref_dq_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# @asset(ins={"dbt_output": AssetIn(key="locations")})
# def run_spark_job(dbt_output):
#     # Assuming dbt_output is a list of tuples representing rows of data
#     df = import_data(dbt_output)
#     check_data(df)