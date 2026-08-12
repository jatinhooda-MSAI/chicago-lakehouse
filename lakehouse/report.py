from pathlib import Path

import dagster as dg
import duckdb

BRONZE = Path("data/bronze")
WAREHOUSE = Path("data/warehouse.duckdb")


def runs(instance: dg.DagsterInstance) -> None:
    print("runs")
    for record in reversed(instance.get_run_records()):
        run, tags = record.dagster_run, record.dagster_run.tags
        if "dagster/asset_partition_range_start" in tags:
            partitions = f"{tags['dagster/asset_partition_range_start']}...{tags['dagster/asset_partition_range_end']}"
        else:
            partitions = tags.get("dagster/partition", "unpartitioned")
        minutes = (record.end_time - record.start_time) / 60
        print(f"  {run.job_name} {partitions}: {run.status.value}, {len(run.asset_selection or [])} assets, {minutes:.1f} min")
        events = instance.all_logs(run.run_id, of_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION)
        evaluations = [e.dagster_event.event_specific_data for e in events]
        print(f"    checks passed {sum(e.passed for e in evaluations)} of {len(evaluations)}")
        for e in evaluations:
            if e.passed and e.check_name != "volume":
                continue
            status = "PASS" if e.passed else e.severity.value
            print(f"    {status} {e.asset_key.to_user_string()} {e.check_name} {e.partition or ''}: {e.description or ''}")
            for key in ("partitions_checked", "out_of_band", "last_partition"):
                if key in e.metadata:
                    print(f"      {key}: {e.metadata[key].value}")
            if e.metadata.get("flagged") and e.metadata["flagged"].value:
                print("      flagged:\n        " + e.metadata["flagged"].value.replace("\n", "\n        "))


def layers() -> None:
    print("bronze")
    for source in ("crimes", "requests_311", "community_areas"):
        files, rows, median, biggest, most, size = duckdb.sql(
            "select count(*), sum(num_rows), median(num_rows), arg_max(file_name, num_rows), max(num_rows), sum(file_size_bytes) "
            f"from parquet_file_metadata('{BRONZE}/{source}/*.parquet')"
        ).fetchone()
        print(f"  {source}: {files} files, {rows} rows, median {median:g} rows per file, largest {Path(biggest).name} with {most} rows, {size / 1e6:.1f} MB")
    print("warehouse")
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    for schema, table in con.execute("select schema_name, table_name from duckdb_tables() order by schema_name desc, table_name").fetchall():
        print(f"  {schema}.{table}: {con.execute(f'select count(*) from {schema}.{table}').fetchone()[0]} rows")
    print(f"  file size {WAREHOUSE.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    runs(dg.DagsterInstance.get())
    layers()
