# StreamVault – ETL Pipeline & Analytics for Sports Streaming

---

## Project Overview

StreamVault is a cloud-based data warehouse (datamart) built for **SportsTV Germany**, a platform that distributes amateur recordings of sporting events. The project integrates data from an operational SQLite database and streaming transaction CSV files into a star schema hosted on a cloud MySQL database, enabling division managers to analyze subscriber behavior, streaming trends, and content performance.

The pipeline follows the **ETL (Extract, Transform, Load)** model, combining the **Kimball Dimensional Table** approach with the **One Big Table** approach to build optimized fact tables for fast analytical queries.

---

## Architecture & Pipeline

```
SQLite (Operational DB)  ──►
                              Extract & Transform (R)  ──►  Star Schema (MySQL)  ──►  Analytics Reports
CSV Streaming Transactions ──►
```

---

## Star Schema Design

The datamart is built around fact tables that support the following analytical use cases:

| Analytics Use Case | Description |
|--------------------|-------------|
| Streaming volume by time period | Total & average transactions per day, week, month, quarter, year |
| Streaming volume by country | Aggregated by geographic region |
| Streaming volume by sport | Aggregated by sport category |
| Total streaming time | By country and by sport across time periods |

### Dimension Tables
- **Date Dimension** – Supports drill-down by day, week, month, quarter, year
- **Sport Dimension** – Sport categories from the operational database
- **Country Dimension** – Subscriber country information

### Fact Tables
- **Streaming Fact Table** – Pre-aggregated streaming transactions with computed metrics for fast analytical queries

---

## Project Structure

```
StreamVault/
│
├── createStarSchema.PractII.MotiwaleK.R      # Designs and creates star schema in MySQL
├── loadAnalyticsDB.PractII.MotiwaleK.R       # ETL: Extracts, transforms & loads data
├── config.R                                  # Database connection configuration
│
├── BusinessAnalysis.PractII.MotiwaleK.Rmd    # Analytics report (R Markdown)
├── BusinessAnalysis.PractII.MotiwaleK.html   # Knitted HTML report
├── BusinessAnalysis.PractII.MotiwaleK.pdf    # Knitted PDF report
│
├── sandbox.Rmd                               # Exploratory sandbox notebook
├── Practicum 2 Self Evaluation.xlsx          # Self-evaluation rubric
│
├── data/                                     # Source data files (not tracked in git)
│   ├── subscribersDB.sqlitedb
│   └── new-streaming-transactions-98732.csv
│
├── .gitignore                                # Excludes sensitive and large files
└── README.md                                 # Project documentation
```

---

## How to Run

### Prerequisites
- R (version 4.0+) and RStudio
- Access to a cloud MySQL database (Aiven or equivalent)
- SQLite database file: `subscribersDB.sqlitedb`
- Streaming transactions CSV: `new-streaming-transactions-98732.csv`
- The following R packages (auto-installed by scripts):
  - `DBI`, `RMySQL`, `RSQLite`

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/kanadmotiwale/StreamVault.git
   ```

2. Create a `data/` subfolder in the project root and place the following files inside it:
   ```
   data/
   ├── subscribersDB.sqlitedb
   └── new-streaming-transactions-98732.csv
   ```

3. Add your database credentials in each R script where indicated:
   ```r
   db_host     <- "YOUR_HOST_HERE"
   db_port     <- YOUR_PORT_HERE
   db_name     <- "YOUR_DB_NAME_HERE"
   db_user     <- "YOUR_USER_HERE"
   db_password <- "YOUR_PASSWORD_HERE"
   ```

### Execution Order
Run the scripts in the following order:

```
1. createStarSchema.PractII.MotiwaleK.R    # Create star schema in MySQL
2. loadAnalyticsDB.PractII.MotiwaleK.R     # Run ETL pipeline to populate fact tables
3. BusinessAnalysis.PractII.MotiwaleK.Rmd  # Knit analytics report to HTML
```

---

## Analytics Report

The `BusinessAnalysis` notebook generates an HTML report with the following sections:

| Section | Description |
|---------|-------------|
| Streaming Growth by Sport | Streaming time and event count growth over all years |
| Weekly Streaming Events | Number of streaming events per week for the most recent year |
| Streams by Sport & Country | Average streaming time and total streams broken down by sport and country |
| Peak Streaming Day | Day of the week with the most streamed content over the past 3 years by sport and country |

---

## ETL Pipeline Details

The ETL process handles:
- Extraction of subscriber and content data from the **SQLite operational database**
- Ingestion of raw **streaming transaction CSV files**
- Transformation into a unified format with consistent date handling
- Batch loading into **MySQL fact and dimension tables** using optimized SQL `INSERT` statements
- Pre-computation of aggregated metrics stored directly in fact tables for fast query performance

Key design decisions:
- Data is **never fully loaded into memory** — SQL aggregations are pushed to the database
- Fact tables are **heavily indexed** for analytical query performance
- Dimensions support **drill-down and roll-up** across time, sport, and country

---

## Security Note

Database credentials are **not** stored in this repository. All connection parameters must be provided locally before running the scripts. The `.gitignore` excludes sensitive files including `.RData`, `.Rhistory`, data files, and RStudio project cache files.

---

## Technologies Used

- **R** – ETL scripting, data transformation, and reporting
- **MySQL (Aiven Cloud)** – Cloud-hosted analytical datamart (star schema)
- **SQLite** – Source operational database for subscriber and content data
- **RMarkdown** – Literate programming and HTML report generation
- **SQL** – Schema design, ETL queries, and analytical aggregations

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
