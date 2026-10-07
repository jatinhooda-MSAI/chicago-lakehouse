# chicago-lakehouse

A small lakehouse on the City of Chicago data portal: Dagster pulls crimes and 311 requests from the Socrata API into daily parquet partitions keyed on modification date, and dbt merges each window into typed silver tables in DuckDB and rebuilds gold facts and a monthly area mart from them. Every run checks freshness, partition volume and the dbt tests, and re-running any date range gives the same warehouse.

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

Python 3.12 and uv; `SOCRATA_APP_TOKEN` is optional.

```
uv sync
uv run dbt parse
mkdir -p data/dagster && export DAGSTER_HOME=$PWD/data/dagster
uv run dagster asset materialize -m lakehouse.definitions --select bronze/community_areas
uv run dagster asset materialize -m lakehouse.definitions \
  --select 'group:bronze or group:silver or group:gold' \
  --partition-range 2025-01-01...2026-10-04
uv run python -m lakehouse.report
uv run dagster dev
```

The backfill is one run: each bronze asset fetches its days, then a single `dbt build` gets the range as `min_date` and `max_date` vars. In the UI, switch on `daily_refresh_schedule` to load yesterday's partition at 23:00 Chicago time. CI builds dbt on the fixtures in `ci/fixtures/`, again incrementally over a window, and checks that the Dagster definitions load (29 s from a clean clone, `results/ci_local.txt`).

## Results

Live API on 2026-10-07, no app token; raw output in `results/`. Backfilling 2025-01-01 through 2026-10-04 (642 daily partitions per source) took 8.1 min. Re-running it on the built warehouse took 7.5 min and left every silver and gold table with identical row counts and content hashes; only the file grew, since DuckDB keeps freed space for reuse. The daily run for 2026-10-05 took 0.9 min. 38 of 40 checks passed in the backfill and 39 of 40 in the daily run; only volume checks warned, crimes on 122 and 311 on 165 of the 641 backfilled partitions with history.

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

## Why these tools

DuckDB instead of a cloud warehouse. The warehouse is one file that the dbt step rebuilds from all of bronze in about a minute, with nothing to provision or pay for. BigQuery or Snowflake earn their cost when many people query at once, data outgrows one machine, or one already exists. The catch is a single writer, so readers should open the file read-only, outside a run.

Dagster instead of Airflow. The work is a set of dated assets, and Dagster has partitions, backfills and asset checks built in, while dagster-dbt makes every model an asset and every dbt test a check. A team that already runs Airflow should use it with Cosmos and catchup rather than add a second orchestrator.

dbt keeps the SQL, contracts, tests and column descriptions together, and `target/manifest.json` is what a downstream SQL agent reads to learn the schema. The metrics in `models/gold/semantic.yml` parse with dbt-core, but nothing here queries them.

Plain parquet instead of Iceberg. Bronze has one writer, replaces whole day files and is small, so parquet read through DuckDB's glob is enough. Iceberg pays off with several writers or engines on the same tables, row-level updates and deletes, snapshot isolation for readers, or time travel.

Daily batch instead of streaming. Crimes reach the portal once a day with a seven day lag and 311 in batches hours late, all through a pull-only API. A stream could not be fresher than the portal and would cost more to run.

## Notes

- Partitions follow modification date, so a revision lands in the partition of the day it happened. The silver merge unions a window's bronze rows with the current silver rows for those ids and keeps the newest, so replaying an old partition never rolls a record back; the `*_is_latest` tests check this.
- The city re-stamps records in bulk: crimes partition 2025-09-23 holds 233,647 rows and 2026-10-01 holds 227,605, against a median of 16. The whole crimes backfill still took 134 s, so I kept modification-date partitions over event-date partitions with a 30 day re-pull, which would miss revisions to older records.
- The volume checks are noisy. 311 warns on nearly every weekend, when volume sits far below a weekday-heavy 14 day median, and crimes warns because a backfill files every record under its last change, leaving old partitions thin. Comparing with the same weekday in earlier weeks would quiet the 311 side.
- Crimes stamps a day's changes around 15:40 and publishes them at 06:00 the next morning; the newest 311 stamp trailed the clock by about nine hours, sometimes out of order (`results/portal_refresh_log.txt`). So the schedule loads yesterday at 23:00, and a partition loaded too early can simply be re-run.
- Records the city deletes never show up in a modification-date pull, so they stay in silver; catching them would take a periodic id reconciliation against the API.
