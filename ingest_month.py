import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import duckdb


archive = Path(sys.argv[1])
output = Path("data/staging") / f"{archive.stem}.parquet"
output.parent.mkdir(parents=True, exist_ok=True)

with ZipFile(archive) as zip_file, TemporaryDirectory(dir=output.parent) as temp_dir:
    csv_members = [
        member for member in zip_file.infolist()
        if not member.is_dir() and member.filename.lower().endswith(".csv")
    ]
    if len(csv_members) != 1:
        raise ValueError(f"Expected one CSV in {archive.name}, found {len(csv_members)}")

    csv_path = Path(temp_dir) / "source.csv"
    parquet_path = Path(temp_dir) / "staging.parquet"

    with zip_file.open(csv_members[0]) as source, csv_path.open("wb") as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)

    with duckdb.connect() as db:
        db.execute(
            """
            COPY (
                SELECT
                    YEAR_MONTH,
                    REGIONAL_OFFICE_CODE,
                    REGIONAL_OFFICE_NAME,
                    ICB_CODE,
                    PRACTICE_CODE,
                    BNF_CHEMICAL_SUBSTANCE_CODE,
                    BNF_CHEMICAL_SUBSTANCE,
                    BNF_PRESENTATION_CODE,
                    BNF_PRESENTATION_NAME,
                    CAST(QUANTITY AS DECIMAL(18, 6)) AS QUANTITY,
                    CAST(TOTAL_QUANTITY AS DECIMAL(18, 6)) AS TOTAL_QUANTITY,   
                    CAST(ITEMS AS BIGINT) AS ITEMS,
                    CAST(ACTUAL_COST AS DECIMAL(18, 4)) AS ACTUAL_COST
                FROM read_csv(?, header = true, delim = ',', all_varchar = true)
                WHERE lower(trim(BNF_CHEMICAL_SUBSTANCE)) IN (
                    'methylphenidate hydrochloride',
                    'lisdexamfetamine dimesylate',
                    'dexamfetamine sulfate'
                )
            ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [str(csv_path), str(parquet_path)],
        )

        counts = db.execute(
            """
            SELECT BNF_CHEMICAL_SUBSTANCE, count(*) AS retained_rows
            FROM read_parquet(?)
            GROUP BY BNF_CHEMICAL_SUBSTANCE
            ORDER BY BNF_CHEMICAL_SUBSTANCE
            """,
            [str(parquet_path)],
        ).fetchall()

    parquet_path.replace(output)

for chemical, count in counts:
    print(f"{chemical}: {count:,} rows")
print(f"Total retained: {sum(count for _, count in counts):,} rows")
print(f"Written to {output}")