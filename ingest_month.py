"""Convert one NHSBSA monthly ZIP into filtered prescribing Parquet.

Detect legacy and current BNF headers and normalise them to a shared staging
schema with YYYY-MM month values. Keep rows for the three selected stimulants
and replace the month’s staged file after conversion succeeds.
"""

import argparse
import csv
import io
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import duckdb


MODERN_BNF_COLUMNS = {
    "BNF_CHEMICAL_SUBSTANCE_CODE": "BNF_CHEMICAL_SUBSTANCE_CODE",
    "BNF_CHEMICAL_SUBSTANCE": "BNF_CHEMICAL_SUBSTANCE",
    "BNF_PRESENTATION_CODE": "BNF_PRESENTATION_CODE",
    "BNF_PRESENTATION_NAME": "BNF_PRESENTATION_NAME",
}
LEGACY_BNF_COLUMNS = {
    "BNF_CHEMICAL_SUBSTANCE_CODE": "BNF_CHEMICAL_SUBSTANCE",
    "BNF_CHEMICAL_SUBSTANCE": "CHEMICAL_SUBSTANCE_BNF_DESCR",
    "BNF_PRESENTATION_CODE": "BNF_CODE",
    "BNF_PRESENTATION_NAME": "BNF_DESCRIPTION",
}
GEOGRAPHY_COLUMNS = (
    "REGIONAL_OFFICE_CODE",
    "REGIONAL_OFFICE_NAME",
    "PRACTICE_CODE",
)


def main():
    parser = argparse.ArgumentParser(description="Stage one monthly NHSBSA ZIP.")
    parser.add_argument("archive", type=Path)
    archive = parser.parse_args().archive

    output = Path("data/staging") / f"{archive.stem}.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(archive) as zip_file, TemporaryDirectory(dir=output.parent) as temp_dir:
        members = [
            member for member in zip_file.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".csv")
        ]
        if len(members) != 1:
            raise ValueError(f"Expected one CSV in {archive.name}, found {len(members)}")

        with zip_file.open(members[0]) as source:
            with io.TextIOWrapper(source, encoding="utf-8-sig", newline="") as text:
                headers = set(next(csv.reader(text), []))

        if set(MODERN_BNF_COLUMNS.values()) <= headers:
            bnf_columns = MODERN_BNF_COLUMNS
            schema = "current"
        elif set(LEGACY_BNF_COLUMNS.values()) <= headers:
            bnf_columns = LEGACY_BNF_COLUMNS
            schema = "legacy"
        else:
            raise ValueError(f"Unrecognised BNF column layout in {archive.name}")

        required = {
            "YEAR_MONTH", "QUANTITY", "TOTAL_QUANTITY", "ITEMS", "ACTUAL_COST",
            *GEOGRAPHY_COLUMNS,
        }
        missing = required - headers
        if missing:
            raise ValueError(f"Missing columns in {archive.name}: {', '.join(sorted(missing))}")

        print(f"{archive.name}: detected {schema} schema.", flush=True)
        csv_path = Path(temp_dir) / "source.csv"
        parquet_path = Path(temp_dir) / "staging.parquet"

        with zip_file.open(members[0]) as source, csv_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)

        bnf_select = ",\n                    ".join(
            f"{source} AS {target}" for target, source in bnf_columns.items()
        )
        chemical_name = bnf_columns["BNF_CHEMICAL_SUBSTANCE"]

        with duckdb.connect() as db:
            db.execute(
                f"""
                COPY (
                    SELECT
                        strftime(
                            strptime(replace(trim(YEAR_MONTH), '-', ''), '%Y%m'),
                            '%Y-%m'
                        ) AS YEAR_MONTH,
                        REGIONAL_OFFICE_CODE,
                        REGIONAL_OFFICE_NAME,
                        {"ICB_CODE" if "ICB_CODE" in headers else "CAST(NULL AS VARCHAR)"} AS ICB_CODE,
                        PRACTICE_CODE,
                        {bnf_select},
                        CAST(QUANTITY AS DECIMAL(18, 6)) AS QUANTITY,
                        CAST(TOTAL_QUANTITY AS DECIMAL(18, 6)) AS TOTAL_QUANTITY,
                        CAST(ITEMS AS BIGINT) AS ITEMS,
                        CAST(ACTUAL_COST AS DECIMAL(18, 4)) AS ACTUAL_COST
                    FROM read_csv(?, header = true, delim = ',', all_varchar = true)
                    WHERE lower(trim({chemical_name})) IN (
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
                SELECT BNF_CHEMICAL_SUBSTANCE, count(*)
                FROM read_parquet(?)
                GROUP BY BNF_CHEMICAL_SUBSTANCE
                ORDER BY BNF_CHEMICAL_SUBSTANCE
                """,
                [str(parquet_path)],
            ).fetchall()

        if not counts:
            raise ValueError(f"No matching stimulant records in {archive.name}")

        parquet_path.replace(output)

    for chemical, count in counts:
        print(f"{chemical}: {count:,} rows")
    print(f"Total retained: {sum(count for _, count in counts):,} rows")
    print(f"Written to {output}")


if __name__ == "__main__":
    main()