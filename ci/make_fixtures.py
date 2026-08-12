from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BRONZE, OUT = Path("data/bronze"), Path("ci/fixtures")
DAYS = ["2026-10-03", "2026-10-04", "2026-10-05"]
STALE_DAY = "2026-10-02"
MODIFIED = {"crimes": "updated_on", "requests_311": "last_modified_date"}

for name, modified in MODIFIED.items():
    (OUT / name).mkdir(parents=True, exist_ok=True)
    for day in DAYS:
        pq.write_table(pq.read_table(BRONZE / name / f"dt={day}.parquet").slice(0, 100), OUT / name / f"dt={day}.parquet")
    stale = pq.read_table(OUT / name / f"dt={DAYS[-1]}.parquet").slice(0, 5)
    stale = stale.set_column(stale.schema.get_field_index(modified), modified, pa.array([f"{STALE_DAY}T12:00:00.000"] * 5))
    pq.write_table(stale, OUT / name / f"dt={STALE_DAY}.parquet")

(OUT / "community_areas").mkdir(parents=True, exist_ok=True)
pq.write_table(pq.read_table(BRONZE / "community_areas" / "community_areas.parquet"), OUT / "community_areas" / "community_areas.parquet")
