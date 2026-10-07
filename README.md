# chicago-lakehouse

A small lakehouse on the City of Chicago data portal. Dagster pulls crimes and 311 service requests from the Socrata API into daily parquet partitions keyed on each record's modification date, dbt merges each partition window into typed silver tables in DuckDB and rebuilds gold facts and a monthly area mart from them, and every run checks freshness, partition volume and the dbt tests. Re-running any date range rewrites those bronze files and merges the same rows again, so the warehouse comes out the same.

```
Socrata API: crimes ijzp-q8t2, 311 v6vf-nfxy, community areas igwz-8jzy
   |  Dagster asset per source, daily partitions on updated_on / last_modified_date
   v
bronze   data/bronze/<source>/dt=YYYY-MM-DD.parquet     raw values, one file per day, replaced on re-run
   |  dbt incremental, unique_key = record id, partition window passed in as vars
   v
silver   silver.stg_crimes, silver.stg_311_requests     typed, newest version of every record
   |  dbt tables, rebuilt every run
   v
gold     gold.dim_community_area, gold.fct_crimes_daily, gold.fct_311_daily, gold.mart_area_month
```

## Running it

Python 3.12 and uv. `SOCRATA_APP_TOKEN` is used if set; without it requests go through Socrata's shared anonymous limit, which was fine for everything below.

```
uv sync
uv run dbt parse
mkdir -p data/dagster && export DAGSTER_HOME=$PWD/data/dagster
uv run dagster asset materialize -m lakehouse.definitions --select bronze/community_areas
uv run dagster asset materialize -m lakehouse.definitions \
  --select 'group:bronze or group:silver or group:gold' \
  --partition-range 2025-01-01...2026-10-04
uv run python -m lakehouse.report
```

The second `dagster asset materialize` is the backfill. Every partitioned asset has a single-run backfill policy, so a range is one run: each bronze asset fetches its days four at a time and writes one file per day, then dbt runs `dbt build` once with `min_date` and `max_date` set to the edges of the range. A single day is `--partition 2026-10-05`. The report prints each run's wall time and check results, rows and size per layer, and a content hash per warehouse table.

`uv run dagster dev` opens the UI on localhost:3000, where the same backfill can be launched from the asset graph by picking a date range. `daily_refresh_schedule` runs the `daily_refresh` job (both bronze sources plus every dbt model) at 23:00 Chicago time for yesterday's partition; switch it on under Automation and the daemon inside `dagster dev` fires it. The freshness sensor next to it re-evaluates the freshness checks between runs.

CI (`.github/workflows/ci.yml`) syncs with uv, runs `dbt build --target ci` against the parquet files in `ci/fixtures/`, runs an incremental `dbt build` over a window of those fixtures, and checks that the Dagster definitions load. The fixtures are 100 real rows per day for three days per source plus five older versions of some of those records, so the dedupe has something to do; `ci/make_fixtures.py` cuts them from a local bronze. The same steps from a clean clone pass locally in 29 s with a warm uv cache (`results/ci_local.txt`).

## Results

One real run on 2026-10-07 against the live API, without an app token. The raw output is in `results/`: CLI logs for each run, `report.txt` from `lakehouse.report` after the last run, and the reports taken right after the backfill and after its re-run.

The backfill of 2025-01-01 through 2026-10-04 (642 daily partitions per source) into an empty warehouse took 552 s of wall time. The two bronze steps ran side by side (311 in 393 s, crimes in 134 s) and the Dagster step around the single dbt build took 66 s, of which dbt's own timer counted 4.3 s. All 34 dbt tests and the 4 freshness checks passed; both volume checks warned, crimes on 122 and 311 on 165 of the 641 partitions that had history.

Re-running the same range on the built warehouse took 515 s, with dbt on its incremental path. Every silver and gold table came back with the same row count and content hash. Bronze 311 came back 5 rows smaller because the portal's answers moved in between: requests that changed again since the first pass now belong to a later partition, and one row of 2026-07-23 that the first pass got was missing from the second answer (compare the per-day counts in the two logs). Silver keeps the previous version of all five, and re-running a partition repairs its bronze file.

The daily run, the `daily_refresh` job for 2026-10-05 (the partition the 23:00 tick on Oct 6 targets), took 62 s: 696 crime rows and 7,417 311 rows, dbt step 17 s. 39 of 40 checks passed; the crimes volume check warned at 696 rows against a trailing median of 22.5 (see the notes). Backfilling through 2026-10-04 and then running 2026-10-05 left `silver.stg_crimes` and `gold.fct_crimes_daily` with exactly the same hashes as an earlier attempt that backfilled through 2026-10-05 in one go (`results/report_full_range_attempt.txt`).

| | after backfill | after daily run |
|---|---|---|
| bronze crimes | 822,849 rows, 642 files, 20.9 MB | 823,545 rows, 643 files, 20.9 MB |
| bronze 311 | 2,539,803 rows, 642 files, 60.4 MB | 2,547,215 rows, 643 files, 60.6 MB |
| silver.stg_crimes | 822,849 | 823,545 |
| silver.stg_311_requests | 2,539,803 | 2,547,220 |
| gold.dim_community_area | 78 | 78 |
| gold.fct_crimes_daily | 225,058 | 225,423 |
| gold.fct_311_daily | 904,496 | 907,181 |
| gold.mart_area_month | 1,716 | 1,716 |
| data/warehouse.duckdb | 231.5 MB | 444.3 MB |

The file nearly doubled with the re-run because rewriting every row leaves the old blocks free inside it; DuckDB reuses them but does not give them back to the disk.

## Why these tools

DuckDB instead of a cloud warehouse. The whole warehouse is one file (231.5 MB after the backfill) and dbt builds silver and gold from all of bronze in seconds on a laptop, so there is nothing to provision, secure or pay for. BigQuery or Snowflake earn their cost when many people or services query at once, when data outgrows one machine, or when the organization already runs one; dbt-duckdb then becomes that adapter and the models move over with small SQL changes. What DuckDB costs here is a single writer: readers such as a SQL agent should open the file read-only, outside a run.

Dagster instead of Airflow. The work is naturally a set of dated assets, and Dagster has partitions, backfills, asset checks and freshness built in, while dagster-dbt turns each model into an asset and each dbt test into a check, so lineage runs from the Socrata pull to the mart in one graph. Airflow can do the same with Cosmos for dbt and catchup for backfills; a team that already operates Airflow should do that rather than run a second orchestrator.

dbt keeps the SQL, contracts, tests and column descriptions in one place, and its `target/manifest.json` is what a downstream SQL agent reads to learn the schema. The semantic models and metrics in `models/gold/semantic.yml` (incidents, arrests, arrest_rate, requests_opened, median_days_to_close, with a day time spine) parse with dbt-core; nothing here queries them through MetricFlow.

Plain parquet instead of Iceberg. Bronze has one writer, replaces whole day files and is small (20.9 MB for crimes, 60.6 MB for 311), so a directory of parquet files read through DuckDB's glob is enough. Iceberg pays off when several writers or engines (Spark, Trino, Snowflake) share tables, when rows need updating or deleting in the lake itself, when readers need snapshot isolation during writes, or for time travel; then silver would move out of the DuckDB file into Iceberg tables.

Daily batch instead of streaming. Crimes reach the portal once a day with a seven day lag, and 311 changes arrive a few times a day, hours late, through a pull-only API. A stream could not be fresher than the portal and would cost more to run. Minute-level freshness would need a feed from the city's source systems.

## Notes

- Partitions follow modification date, not event date, so a revision lands in the partition of the day it happened and the silver merge keeps the newest version of each id. The merge unions the window's bronze rows with the current silver rows for the same ids and keeps the latest, so replaying an old partition can never roll a record back. The `*_is_latest` tests check that silver holds every bronze record at its newest version.
- The city re-stamps records in bulk. The largest crimes partition, 2025-09-23, holds 233,647 rows against a median of 16, and 2026-10-01 holds 227,605. The whole crimes backfill, those days included, still took 134 s, so I kept modification-date partitions instead of the fallback (event-date partitions with a 30 day re-pull), which would also miss revisions to anything older than 30 days.
- The API only serves the current version of a record, so a backfill files every record under the date of its last change. Old partitions come out thin and re-stamp days huge, which is why the crimes volume check flags so many backfilled partitions. Loaded day by day, a partition holds what changed that day.
- The 311 volume check flags nearly every weekend and holiday, because weekend volume sits far below a weekday-heavy 14 day median. A same-weekday baseline would fix that; I kept the plain median.
- Crimes stamps a day's changes at about 15:40 and publishes them at 06:00 the next morning. 311 lands in batches through the day; during the run its newest stamp trailed the clock by about nine hours (`results/portal_refresh_log.txt`), and changes did not always arrive in stamp order. That is why the schedule fires at 23:00 for yesterday's partition, long after both have landed. A partition run before its data lands comes back short, and re-running it is safe.
- Silver holds every record changed since 2025-01-01, including much older incidents the city revised in that period. Gold only counts events on or after the `start_date` var, where coverage is complete because a record cannot be modified before it exists.
- Records the city deletes never show up in a modification-date pull, so they stay in silver. Catching them would take a periodic id reconciliation against the API.
- Records with no community area, or area 0, map to an UNKNOWN member (0) in `dim_community_area`, so fact keys are never null. Information-only 311 calls are dropped in the API query, and the 311 facts skip requests marked as duplicates.
- Gold models carry the same daily partitions as silver because they are built in the same dbt invocation; every run rebuilds them whole. A range run records one volume result for the range, a single-day run records it against that day.
- Dagster 1.13 marks `build_last_update_freshness_checks` as superseded by `FreshnessPolicy`. I kept the checks because they run on demand and sit next to the volume checks and dbt tests. The CLI prints that SupersessionWarning and a BetaWarning for `backfill_policy`.
