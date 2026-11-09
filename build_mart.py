from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb


staging_dir = Path("data/staging")
output = Path("data/marts/adhd_prescribing_monthly.csv")
staged_files = sorted(staging_dir.glob("EPD_SNOMED_*.parquet"))

if not staged_files:
    raise SystemExit(f"No monthly Parquet files found in {staging_dir}")

with duckdb.connect() as db:
    db.read_parquet([str(path) for path in staged_files]).create_view("staged_files")

    db.execute("""
        CREATE TEMP VIEW staging AS
        SELECT
            strptime(YEAR_MONTH, '%Y-%m')::DATE AS month,
            *
        FROM staged_files
    """)

    db.execute("""
        CREATE TEMP TABLE mart AS
        SELECT
            month,
            REGIONAL_OFFICE_CODE AS region_code,
            REGIONAL_OFFICE_NAME AS region_name,
            BNF_CHEMICAL_SUBSTANCE_CODE AS chemical_code,
            BNF_CHEMICAL_SUBSTANCE AS chemical_name,
            BNF_PRESENTATION_CODE AS presentation_code,
            BNF_PRESENTATION_NAME AS presentation_name,
            sum(ITEMS) AS total_items,
            sum(ACTUAL_COST) AS total_actual_cost,
            sum(TOTAL_QUANTITY) AS total_quantity
        FROM staging
        GROUP BY ALL
    """)

    mismatches = db.execute("""
        WITH staging_totals AS (
            SELECT
                month,
                sum(ITEMS) AS items,
                sum(ACTUAL_COST) AS cost,
                sum(TOTAL_QUANTITY) AS quantity
            FROM staging
            GROUP BY month
        ),
        mart_totals AS (
            SELECT
                month,
                sum(total_items) AS items,
                sum(total_actual_cost) AS cost,
                sum(total_quantity) AS quantity
            FROM mart
            GROUP BY month
        )
        SELECT coalesce(s.month, m.month)
        FROM staging_totals AS s
        FULL JOIN mart_totals AS m USING (month)
        WHERE s.month IS NULL OR m.month IS NULL
           OR s.items IS DISTINCT FROM m.items
           OR s.cost IS DISTINCT FROM m.cost
           OR s.quantity IS DISTINCT FROM m.quantity
    """).fetchall()

    if mismatches:
        raise ValueError(f"Mart totals differ from staging for {mismatches}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output.parent) as temp_dir:
        temporary_output = Path(temp_dir) / output.name
        db.execute("""
            COPY (
                SELECT *
                FROM mart
                ORDER BY month, region_code, chemical_code, presentation_code
            ) TO ? (FORMAT CSV, HEADER TRUE)
        """, [str(temporary_output)])
        temporary_output.replace(output)

    row_count = db.execute("SELECT count(*) FROM mart").fetchone()[0]

print(f"Monthly items, cost, and quantity match staging.")
print(f"Wrote {row_count:,} rows to {output}")