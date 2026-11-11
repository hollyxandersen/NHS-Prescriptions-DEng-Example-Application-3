# The Information Lab - DEng Data School Application Example 3 - NHS Prescriptions

**Note — About this example**

> This project was developed as an example for The Information Lab's Data Engineering School application process.
>
> It demonstrates how to build a pipeline around large bulk files without using a data API. The pipeline processes monthly NHSBSA ZIP archives one at a time, standardises historical file layouts, and uses DuckDB and Parquet to turn several years of prescribing data into a compact analytical dataset.
>
> This example also demonstrates a different output format. The pipeline produces a CSV for a Tableau Public dashboard.

## 1. What I built and who for

I built a Python and DuckDB pipeline to explore how dispensing of three stimulants used to treat ADHD changed in England from November 2020 to August 2026. The dashboard is for anyone interested in changes in dispensing volumes, costs and the mix of brands around the September 2023 shortage alert.

The dashboard pairs a bump chart showing changes in brand rank with a line chart showing monthly quantities dispensed. Together, they show which brands moved up or down and the scale of the changes in their recorded quantities. Supporting charts show the overall dispensing trend, average cost per unit and when brands first appear in the data.

**[View the dashboard on Tableau Public](https://public.tableau.com/app/profile/holly.andersen/viz/2023ADHDMedicationShortage-NHSPrescriptionDataPipeline/ADHDMedicationDashboard)**

![NHS prescriptions dashboard overview](images/ADHD%20Medication%20Dashboard.png)

## 2. The data

The source is the [NHSBSA English Prescribing Dataset with SNOMED Code](https://opendata.nhsbsa.net/dataset/english-prescribing-dataset-epd-with-snomed-code). NHSBSA publishes monthly bulk downloads, with recent ZIPs around 650 MB each.

This project retains records for three chemical substances:

- Dexamfetamine sulfate
- Lisdexamfetamine dimesylate
- Methylphenidate hydrochloride

The staging files retain the month, geography, BNF chemical and presentation identifiers, item counts, quantities and actual costs.

The dashboard uses total quantity for brand ranks so that, for example, four items of seven tablets count the same as one item of 28 tablets. Quantities represent presentation-specific units, such as tablets, capsules or millilitres. They do not measure patients or standardised daily doses.

The data covers prescriptions dispensed in the community and excludes unfilled prescriptions. Records under generic names do not identify the manufacturer supplying the medicine. A brand's first appearance means its first observed month in this dataset. The September 2023 alert provides a reference point for exploring changes over time.

NHSBSA Copyright 2021-2026. Licensed under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## 3. How it works

### Downloading and resuming a backfill

`run_pipeline.py` accepts a single month or an inclusive date range. It discovers signed ZIP links from the NHSBSA dataset page, reuses local archives where available, and skips months that already have a staged Parquet file.

The runner downloads one archive at a time into a `.part` file and renames it when the transfer completes. Interrupted transfers are retried up to twice per URL, with waits of two and four seconds. If a download returns HTTP 400 or 403, the runner refreshes the signed links once and retries.

If the run stops, running the same command again picks up from the staged months. Each retry restarts the interrupted ZIP download from the beginning.

ZIPs downloaded by the current run are removed after their staged output passes validation. Archives already present when the run starts are retained, including downloads left behind by an earlier ingest failure.

### Standardising and staging each month

`ingest_month.py` inspects the CSV header to identify the source layout.

Older releases use different BNF column names and a `YYYYMM` month value. The ingest maps older and newer layouts to a shared set of column names and a `YYYY-MM` month format.

Earlier releases also lack `ICB_CODE`. The pipeline preserves a consistent staging schema by writing a null value when this field is absent. The Tableau dataset uses regional geography.

The script temporarily extracts the CSV, filters it to the three selected medicines and writes one compressed Parquet file per month. Codes remain text, while quantities, items and costs are converted to numeric types.

The Parquet is written to a temporary location before replacing the month's staged file. The runner then checks that the output contains rows and that all retained rows belong to the requested month.

### Building and validating the Tableau dataset

`build_mart.py` reads all staged Parquet files and produces one row per month, region and BNF presentation. It retains the parent chemical and presentation labels and sums items, total quantity and actual cost.

Before replacing `data/marts/adhd_prescribing_monthly.csv`, it checks that monthly item counts, quantities and actual costs match the staging data.

The date range passed to the runner controls which months are ingested. The final CSV includes all months present in staging.

### Testing download recovery

`test_run_pipeline.py` simulates a connection reset, persistent timeouts and an HTTP 403 without making network requests or waiting through real retry delays.

The tests check that:

- A download succeeds after a simulated connection reset.
- Repeated timeouts stop after three attempts per URL.
- An HTTP 403 passes back to the runner's signed-link handling.

During development, I also checked that August 2026's filtered row count matched the NHSBSA explorer and that rerunning a staged month preserved the Tableau totals.

## 4. How to run it

The project requires Python and DuckDB. Run the commands from the repository root.

Create and activate a virtual environment:

```bash
python -m venv .venv
```

On Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

On macOS or Linux:

```bash
source .venv/bin/activate
```

Install DuckDB:

```bash
python -m pip install duckdb
```

To reproduce the dashboard's full date range:

```bash
python run_pipeline.py 2020-11 2026-08
```

The full backfill downloads several dozen GB of source archives. Allow enough free disk space for one bulk ZIP, its temporary expanded CSV and the retained Parquet files.

To resume an interrupted run, repeat the same command. Months with an existing staged Parquet are skipped.

To run the unit tests:

```bash
python -m unittest -v test_run_pipeline
```

Connect Tableau to:

```text
data/marts/adhd_prescribing_monthly.csv
```

Generated data is excluded from Git through the `/data/` entry in `.gitignore`.

## 5. What I would do next

- Add a run manifest recording the source month, detected schema, retained row count and completion status. This would make runs easier to audit and provide stronger evidence for deciding which months to skip.
- Add small fixture ZIPs for historical and current layouts to test ingestion and mart reconciliation together.
- Compare direct streaming with temporary CSV extraction to measure the trade-off in disk usage and runtime.
- Add an explicit reprocessing option for refreshing a month after source revisions or changes to the ingest logic.

## 6. Where AI helped

I used AI to help draft and revise the Python scripts, and troubleshoot differences between historical file layouts.
