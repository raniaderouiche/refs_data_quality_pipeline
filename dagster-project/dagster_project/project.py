from pathlib import Path

from dagster_dbt import DbtProject

ref_dq = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..","..","dbt", "ref_dq").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..","..", "dbt/ref_dq/dbt-project").resolve(),
)
ref_dq.prepare_if_dev()