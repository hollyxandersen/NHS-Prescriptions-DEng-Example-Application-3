import argparse
import re
import shutil
import subprocess
import sys
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit
from urllib.request import urlopen

import duckdb


DATASET_URL = (
    "https://opendata.nhsbsa.net/dataset/"
    "english-prescribing-dataset-epd-with-snomed-code"
)
ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw"
STAGING = ROOT / "data" / "staging"


class ZipLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.by_month = {}

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return

        href = dict(attrs).get("href")
        if not href:
            return

        url = urljoin(DATASET_URL, href)
        parts = urlsplit(url)
        filename = unquote(parts.path).rsplit("/", 1)[-1]
        match = re.fullmatch(r"EPD_SNOMED_(\d{6})\.zip", filename, re.IGNORECASE)
        if match and parts.scheme == "https":
            self.by_month[match[1]] = url


def parse_month(value):
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise argparse.ArgumentTypeError("Use YYYY-MM, for example 2026-06.")
    try:
        return date.fromisoformat(f"{value}-01")
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"Invalid month: {value}") from error


def download_zip(url, archive):
    partial = archive.with_suffix(".part")
    try:
        with urlopen(url, timeout=60) as response, partial.open("wb") as target:
            shutil.copyfileobj(response, target, length=1024 * 1024)
        partial.replace(archive)
    finally:
        partial.unlink(missing_ok=True)


def run_script(script, *args):
    subprocess.run([sys.executable, script, *args], cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description="Build the Tableau prescribing mart.")
    parser.add_argument("start", type=parse_month)
    parser.add_argument("end", type=parse_month, nargs="?")
    args = parser.parse_args()
    end = args.end or args.start
    if end < args.start:
        parser.error("End month must be on or after start month.")

    RAW.mkdir(parents=True, exist_ok=True)
    links = None
    start_index = args.start.year * 12 + args.start.month - 1
    end_index = end.year * 12 + end.month - 1

    with duckdb.connect() as db:
        for index in range(start_index, end_index + 1):
            year, month_index = divmod(index, 12)
            month = f"{year:04d}-{month_index + 1:02d}"
            stem = f"EPD_SNOMED_{month.replace('-', '')}"
            archive = RAW / f"{stem}.ZIP"
            parquet = STAGING / f"{stem}.parquet"

            if parquet.is_file():
                print(f"{month}: already staged, skipping.", flush=True)
                continue

            downloaded = not archive.is_file()
            if downloaded:
                if links is None:
                    page = ZipLinks()
                    with urlopen(DATASET_URL, timeout=60) as response:
                        page.feed(response.read().decode("utf-8"))
                    links = page.by_month

                url = links.get(month.replace("-", ""))
                if url is None:
                    raise SystemExit(f"No official ZIP link found for {month}.")

                print(f"{month}: downloading bulk ZIP.", flush=True)
                download_zip(url, archive)

            run_script("ingest_month.py", str(archive))

            rows, wrong_months = db.execute(
                """
                SELECT
                    count(*),
                    count(*) FILTER (WHERE YEAR_MONTH IS DISTINCT FROM ?)
                FROM read_parquet(?)
                """,
                [month, str(parquet)],
            ).fetchone()

            if not rows or wrong_months:
                parquet.unlink()
                raise SystemExit(f"{month}: staging check failed. ZIP kept for inspection.")

            print(f"{month}: validated {rows:,} staged rows.", flush=True)
            if downloaded:
                archive.unlink()

    run_script("build_mart.py")


if __name__ == "__main__":
    main()