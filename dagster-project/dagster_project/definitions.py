from dagster import Definitions, load_assets_from_modules
import sys
import os

from dagster_dbt import DbtCliResource

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from dagster_project import assets  # noqa: TID252

from .assets import ref_dq_dbt_assets, spark_session
from .project import ref_dq

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(project_dir=ref_dq),"spark_session": spark_session
    },  
)
