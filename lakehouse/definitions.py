import json
from datetime import timedelta
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

from lakehouse.bronze import community_areas, crimes, daily, requests_311

project = DbtProject(project_dir=Path(__file__).parent.parent)
project.prepare_if_dev()


@dbt_assets(manifest=project.manifest_path, project=project, partitions_def=daily, backfill_policy=dg.BackfillPolicy.single_run())
def warehouse(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    window = context.partition_time_window
    dbt_vars = {"min_date": f"{window.start:%Y-%m-%d}", "max_date": f"{window.end:%Y-%m-%d}"}
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()


freshness = dg.build_last_update_freshness_checks(
    assets=[
        crimes,
        requests_311,
        get_asset_key_for_model([warehouse], "stg_crimes"),
        get_asset_key_for_model([warehouse], "stg_311_requests"),
    ],
    lower_bound_delta=timedelta(hours=26),
)

daily_refresh = dg.define_asset_job("daily_refresh", selection=[crimes, requests_311, warehouse], partitions_def=daily)

defs = dg.Definitions(
    assets=[community_areas, crimes, requests_311, warehouse],
    asset_checks=freshness,
    jobs=[daily_refresh],
    schedules=[dg.build_schedule_from_partitioned_job(daily_refresh, hour_of_day=9)],
    sensors=[dg.build_sensor_for_freshness_checks(freshness_checks=freshness)],
    resources={"dbt": DbtCliResource(project_dir=project)},
)
