import os
import statistics
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRONZE = Path("data/bronze")
PAGE = 50000

daily = dg.DailyPartitionsDefinition(start_date="2025-01-01", timezone="America/Chicago")

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=8, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))
if token := os.environ.get("SOCRATA_APP_TOKEN"):
    session.headers["X-App-Token"] = token

STR, BOOL = pa.string(), pa.bool_()
CRIMES = pa.schema({
    "id": STR, "case_number": STR, "date": STR, "block": STR, "iucr": STR, "primary_type": STR,
    "description": STR, "location_description": STR, "arrest": BOOL, "domestic": BOOL, "beat": STR,
    "district": STR, "ward": STR, "community_area": STR, "fbi_code": STR, "updated_on": STR,
})
REQUESTS_311 = pa.schema({
    "sr_number": STR, "sr_type": STR, "sr_short_code": STR, "owner_department": STR, "status": STR,
    "origin": STR, "created_date": STR, "closed_date": STR, "last_modified_date": STR, "duplicate": BOOL,
    "legacy_record": BOOL, "parent_sr_number": STR, "community_area": STR, "ward": STR,
})
COMMUNITY_AREAS = pa.schema({"area_numbe": STR, "community": STR})


def fetch(dataset: str, schema: pa.Schema, where: str | None = None) -> pa.Table:
    pages = []
    while True:
        response = session.get(
            f"https://data.cityofchicago.org/resource/{dataset}.json",
            params={"$select": ",".join(schema.names), "$where": where, "$order": ":id", "$limit": PAGE, "$offset": PAGE * len(pages)},
            timeout=600,
        )
        response.raise_for_status()
        rows = response.json()
        pages.append(pa.Table.from_pylist(rows, schema=schema))
        if len(rows) < PAGE:
            return pa.concat_tables(pages)


def write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)


def volume_check(name: str, keys: list[str]) -> dg.AssetCheckResult:
    counts = {p.stem.removeprefix("dt="): pq.read_metadata(p).num_rows for p in (BRONZE / name).glob("dt=*.parquet")}
    checked, flagged = [], []
    for key in keys:
        day = date.fromisoformat(key)
        history = [counts[d] for i in range(1, 15) if (d := str(day - timedelta(days=i))) in counts]
        if history:
            median = statistics.median(history)
            checked.append(f"{key}: {counts[key]} rows, trailing median {median:g}")
            if not 0.5 * median <= counts[key] <= 2 * median:
                flagged.append(checked[-1])
    metadata = {"partitions_checked": len(checked), "out_of_band": len(flagged), "flagged": "\n".join(flagged)}
    if checked:
        metadata["last_partition"] = checked[-1]
    return dg.AssetCheckResult(check_name="volume", passed=not flagged, severity=dg.AssetCheckSeverity.WARN, metadata=metadata)


def bronze_asset(name: str, dataset: str, schema: pa.Schema, modified: str, extra: str = "") -> dg.AssetsDefinition:
    @dg.asset(
        key=["bronze", name],
        group_name="bronze",
        partitions_def=daily,
        backfill_policy=dg.BackfillPolicy.single_run(),
        check_specs=[
            dg.AssetCheckSpec(
                "volume",
                asset=["bronze", name],
                partitions_def=daily,
                description="Rows per partition vs the median of the previous 14 daily partitions; warns outside 0.5x to 2x.",
            )
        ],
        kinds={"socrata", "parquet"},
        description=f"Rows of Socrata dataset {dataset} whose {modified} falls on the partition day, one parquet file per day.",
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        def load(key: str) -> int:
            day = date.fromisoformat(key)
            where = f"{modified} >= '{day}T00:00:00' AND {modified} < '{day + timedelta(days=1)}T00:00:00'{extra}"
            table = fetch(dataset, schema, where)
            write(table, BRONZE / name / f"dt={key}.parquet")
            return table.num_rows

        keys = context.partition_keys
        rows = 0
        with ThreadPoolExecutor(4) as pool:
            for key, n in zip(keys, pool.map(load, keys)):
                context.log.info(f"{name} {key}: {n} rows")
                rows += n
        return dg.MaterializeResult(metadata={"rows": rows, "partitions": len(keys)}, check_results=[volume_check(name, keys)])

    return _asset


crimes = bronze_asset("crimes", "ijzp-q8t2", CRIMES, "updated_on")
requests_311 = bronze_asset("requests_311", "v6vf-nfxy", REQUESTS_311, "last_modified_date", " AND sr_type != '311 INFORMATION ONLY CALL'")


@dg.asset(key=["bronze", "community_areas"], group_name="reference", kinds={"socrata", "parquet"}, description="The 77 Chicago community areas (number and name) from Socrata dataset igwz-8jzy, geometry dropped.")
def community_areas() -> dg.MaterializeResult:
    table = fetch("igwz-8jzy", COMMUNITY_AREAS)
    write(table, BRONZE / "community_areas" / "community_areas.parquet")
    return dg.MaterializeResult(metadata={"rows": table.num_rows})
