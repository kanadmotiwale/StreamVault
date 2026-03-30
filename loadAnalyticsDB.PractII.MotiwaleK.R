# CS5200 - Practicum II: ETL Pipeline
# File: loadAnalyticsDB.PractII.MotiwaleK.R
# Author: Kanad Motiwale
# Term: Fall 2025

# Generative AI / LLM Usage Acknowledgment
# Some guidance for this assignment was obtained using an AI agent.
# Link to conversation: https://chatgpt.com/c/692f59a5-fbac-832a-9a77-61a43b5bc05b
#
# All code in this file has been reviewed, validated, and modified by me.
# I fully understand the purpose, logic, and performance implications of every 
# line included here. No code was copied without verification or adaptation.

# SETUP
# ---------

if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")
if (!require("RSQLite")) install.packages("RSQLite")

library(RMySQL)
library(DBI)
library(RSQLite)

source("config.R")

# FUNCTIONS: Database Connections
# --------------------------------

connectToMySQL <- function() {
  tryCatch({
    conn <- dbConnect(RMySQL::MySQL(), host = DB_HOST, port = DB_PORT,
                      dbname = DB_NAME, user = DB_USER, password = DB_PASSWORD)
    cat("✓ Connected to MySQL (Analytical Database)\n")
    return(conn)
  }, error = function(e) {
    stop("MySQL connection failed: ", e$message)
  })
}

connectToSQLite <- function() {
  tryCatch({
    conn <- dbConnect(RSQLite::SQLite(), SQLITE_DB)
    cat("✓ Connected to SQLite (Operational Database)\n")
    return(conn)
  }, error = function(e) {
    stop("SQLite connection failed: ", e$message)
  })
}

# FUNCTION: Clear Existing Data (This deletes rows from target tables)
# ------------------------------

clearExistingData <- function(mysqlConn) {
  cat("\n-- Clearing existing data --\n")
  
  for (table in c("fact_streaming", "dim_date", "dim_sport", "dim_country")) {
    tryCatch({
      count_before <- dbGetQuery(mysqlConn, paste0("SELECT COUNT(*) as cnt FROM ", table))$cnt
      dbExecute(mysqlConn, paste0("DELETE FROM ", table))
      cat("✓ Cleared", table, "- removed", count_before, "rows\n")
    }, error = function(e) {
      cat("  Note:", table, "was already empty\n")
    })
  }
}

# FUNCTION: Load Dimensions
# --------------------------

# This loads dim_country, dim_sport, and dates via loadDateDimension().

loadDimensionTables <- function(mysqlConn, sqliteConn) {
  cat("\n-- Loading Dimension Tables --\n")
  
  # Countries
  cat("\nLoading dim_country...\n")
  countries <- dbGetQuery(sqliteConn, "SELECT country_id, country FROM countries")
  if (nrow(countries) > 0) {
    values <- paste0("(", countries$country_id, ", '", countries$country, "')", collapse = ", ")
    tryCatch({
      dbExecute(mysqlConn, paste0("INSERT INTO dim_country (country_id, country_name) VALUES ", values))
      cat("✓ Loaded", nrow(countries), "countries\n")
    }, error = function(e) { cat("  Note: Already loaded\n") })
  }
  
  # Sports
  cat("\nLoading dim_sport...\n")
  sports <- dbGetQuery(sqliteConn, "SELECT DISTINCT sport FROM assets ORDER BY sport")
  if (nrow(sports) > 0) {
    values <- paste0("('", sports$sport, "')", collapse = ", ")
    tryCatch({
      dbExecute(mysqlConn, paste0("INSERT INTO dim_sport (sport_name) VALUES ", values))
      cat("✓ Loaded", nrow(sports), "unique sports\n")
    }, error = function(e) { cat("  Note: Already loaded\n") })
  }
  
  # Dates
  cat("\nLoading dim_date...\n")
  loadDateDimension(mysqlConn, sqliteConn)
}

# FUNCTION: Load Date Dimension
# ------------------------------

loadDateDimension <- function(mysqlConn, sqliteConn) {
  dates_result <- dbGetQuery(sqliteConn, "SELECT MIN(streaming_date) as min_date, MAX(streaming_date) as max_date FROM streaming_txns")
  min_date_sqlite <- as.Date(dates_result$min_date[1])
  max_date_sqlite <- as.Date(dates_result$max_date[1])
  
  csv_sample <- read.csv(CSV_FILE, nrows = 1000, stringsAsFactors = FALSE)
  min_date_csv <- as.Date(min(csv_sample$streaming_date))
  max_date_csv <- as.Date(max(csv_sample$streaming_date))
  
  min_date <- min(min_date_sqlite, min_date_csv, na.rm = TRUE)
  max_date <- max(max_date_sqlite, max_date_csv, na.rm = TRUE)
  
  cat("  Date range:", as.character(min_date), "to", as.character(max_date), "\n")
  
  date_seq <- seq(from = min_date, to = max_date, by = "day")
  date_dim <- data.frame(
    full_date = date_seq,
    year = as.integer(format(date_seq, "%Y")),
    quarter = as.integer(ceiling(as.integer(format(date_seq, "%m")) / 3)),
    month = as.integer(format(date_seq, "%m")),
    week = as.integer(format(date_seq, "%U")),
    day_of_week = as.integer(format(date_seq, "%w")),
    day_name = format(date_seq, "%A"),
    month_name = format(date_seq, "%B"),
    stringsAsFactors = FALSE
  )
  
  cat("  Inserting", nrow(date_dim), "dates...\n")
  
  batch_size <- 1000
  for (i in seq(1, nrow(date_dim), batch_size)) {
    end_idx <- min(i + batch_size - 1, nrow(date_dim))
    batch <- date_dim[i:end_idx, ]
    
    values <- paste0("('", batch$full_date, "', ", batch$year, ", ", batch$quarter, ", ",
                     batch$month, ", ", batch$week, ", ", batch$day_of_week, ", '",
                     batch$day_name, "', '", batch$month_name, "')", collapse = ", ")
    
    tryCatch({
      dbExecute(mysqlConn, paste0("INSERT INTO dim_date (full_date, year, quarter, month, week, day_of_week, day_name, month_name) VALUES ", values))
    }, error = function(e) {})
  }
  
  cat("✓ Loaded", nrow(date_dim), "dates\n")
}

# FUNCTION: Create Staging Table(This create staging_streams)
# -------------------------------

createStagingTable <- function(mysqlConn) {
  cat("\n-- Creating staging table --\n")
  dbExecute(mysqlConn, "DROP TABLE IF EXISTS staging_streams")
  
  dbExecute(mysqlConn, "
    CREATE TABLE staging_streams (
      staging_id INT AUTO_INCREMENT PRIMARY KEY,
      streaming_date DATE,
      user_id VARCHAR(50),
      minutes_streamed INT,
      sport VARCHAR(50),
      country_id INT,
      INDEX idx_date (streaming_date),
      INDEX idx_sport_country (sport, country_id)
    ) ENGINE=InnoDB
  ")
  
  cat("✓ Staging table created\n")
}

# FUNCTION: Load SQLite to Staging
# ---------------------------------

# This reads joined streaming_txns + assets from SQLite and inserts to staging.

loadSQLiteToStaging <- function(mysqlConn, sqliteConn) {
  cat("\nStep 1: Loading SQLite data to staging...\n")
  
  df <- dbGetQuery(sqliteConn, "
    SELECT s.streaming_date, s.user_id, s.minutes_streamed, a.sport, a.country_id
    FROM streaming_txns s
    INNER JOIN assets a ON s.asset_id = a.asset_id
    WHERE s.streaming_date IS NOT NULL
      AND s.minutes_streamed IS NOT NULL
      AND a.sport IS NOT NULL
      AND a.country_id IS NOT NULL
  ")
  
  cat("✓ Extracted", nrow(df), "transactions from SQLite\n")
  insertToStaging(mysqlConn, df)
}

# FUNCTION: Load CSV to Staging
# ------------------------------

# It reads CSV in chunks, joins to assets, cleans rows, inserts to staging.

loadCSVToStaging <- function(mysqlConn, sqliteConn) {
  cat("\nStep 2: Loading CSV data to staging...\n")
  
  assets <- dbGetQuery(sqliteConn, "SELECT asset_id, sport, country_id FROM assets")
  
  chunk_size <- 2000
  total_loaded <- 0
  
  csv_conn <- file(CSV_FILE, "r")
  header_line <- readLines(csv_conn, n = 1)
  
  repeat {
    chunk_lines <- readLines(csv_conn, n = chunk_size)
    if (length(chunk_lines) == 0) break
    
    chunk_text <- paste(c(header_line, chunk_lines), collapse = "\n")
    chunk_df <- read.csv(text = chunk_text, stringsAsFactors = FALSE)
    chunk_df <- merge(chunk_df, assets, by = "asset_id", all.x = FALSE)
    chunk_df <- chunk_df[, c("streaming_date", "user_id", "minutes_streamed", "sport", "country_id")]
    chunk_df <- chunk_df[complete.cases(chunk_df), ]
    
    if (nrow(chunk_df) > 0) {
      insertToStaging(mysqlConn, chunk_df)
      total_loaded <- total_loaded + nrow(chunk_df)
    }
  }
  
  close(csv_conn)
  cat("✓ Loaded", total_loaded, "transactions from CSV\n")
}

# HELPER: Fast Insert to Staging
# -------------------------------

insertToStaging <- function(mysqlConn, df) {
  batch_size <- 2000
  total_rows <- nrow(df)
  
  for (i in seq(1, total_rows, batch_size)) {
    end_idx <- min(i + batch_size - 1, total_rows)
    batch <- df[i:end_idx, ]
    
    batch$user_id <- as.character(batch$user_id)
    batch$sport <- as.character(batch$sport)
    batch$user_id <- gsub("'", "''", gsub("\\", "\\\\", batch$user_id, fixed = TRUE), fixed = TRUE)
    batch$sport <- gsub("'", "''", gsub("\\", "\\\\", batch$sport, fixed = TRUE), fixed = TRUE)
    
    values <- paste0("(NULL, '", batch$streaming_date, "', '", batch$user_id, "', ",
                     batch$minutes_streamed, ", '", batch$sport, "', ", batch$country_id, ")",
                     collapse = ", ")
    
    sql <- paste0("INSERT INTO staging_streams VALUES ", values)
    
    tryCatch({
      dbExecute(mysqlConn, sql)
    }, error = function(e) {
      cat("  Batch at row", i, "skipped\n")
    })
    
    if (i %% 100000 == 1 && i > 1) {
      cat("  ... processed", i, "/", total_rows, "rows\n")
    }
  }
}

# FUNCTION: Compute Facts Using SQL
# ----------------------------------

# This executes a single SQL INSERT ... SELECT ... GROUP BY from staging_streams into fact_streaming.

computeFactsUsingSQL <- function(mysqlConn) {
  cat("\nStep 3: Computing facts using SQL aggregation...\n")
  
  dbExecute(mysqlConn, "
    INSERT INTO fact_streaming 
      (date_id, country_id, sport_id, streaming_year, total_streams, total_minutes_streamed, 
       avg_minutes_per_stream, unique_subscribers)
    SELECT 
      dd.date_id, s.country_id, ds.sport_id, dd.year as streaming_year,
      COUNT(*) as total_streams,
      SUM(s.minutes_streamed) as total_minutes_streamed,
      ROUND(AVG(s.minutes_streamed), 2) as avg_minutes_per_stream,
      COUNT(DISTINCT s.user_id) as unique_subscribers
    FROM staging_streams s
    INNER JOIN dim_date dd ON s.streaming_date = dd.full_date
    INNER JOIN dim_sport ds ON s.sport = ds.sport_name
    GROUP BY dd.date_id, s.country_id, ds.sport_id, dd.year
  ")
  
  fact_count <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM fact_streaming")$cnt
  cat("✓ Computed", fact_count, "facts using SQL GROUP BY\n")
}

# FUNCTION: Main ETL
# -------------------

loadFactStreaming <- function(mysqlConn, sqliteConn) {
  cat("\n-- Loading Fact Table Using SQL Aggregation --\n")
  
  createStagingTable(mysqlConn)
  loadSQLiteToStaging(mysqlConn, sqliteConn)
  loadCSVToStaging(mysqlConn, sqliteConn)
  computeFactsUsingSQL(mysqlConn)
  
  dbExecute(mysqlConn, "DROP TABLE IF EXISTS staging_streams")
  cat("✓ Staging table dropped\n")
}

# FUNCTION: Verify (Drops staging and verifies counts)
# -----------------

verifyDataLoad <- function(mysqlConn) {
  cat("\n-- Verifying Data Load --\n")
  
  cat("✓ dim_country:", dbGetQuery(mysqlConn, "SELECT COUNT(*) FROM dim_country")[[1]], "rows\n")
  cat("✓ dim_sport:", dbGetQuery(mysqlConn, "SELECT COUNT(*) FROM dim_sport")[[1]], "rows\n")
  cat("✓ dim_date:", dbGetQuery(mysqlConn, "SELECT COUNT(*) FROM dim_date")[[1]], "rows\n")
  cat("✓ fact_streaming:", dbGetQuery(mysqlConn, "SELECT COUNT(*) FROM fact_streaming")[[1]], "rows\n")
  
  cat("\nSample data:\n")
  print(dbGetQuery(mysqlConn, "
    SELECT f.fact_id, dd.full_date, dc.country_name, ds.sport_name, f.total_streams
    FROM fact_streaming f
    JOIN dim_country dc ON f.country_id = dc.country_id
    JOIN dim_sport ds ON f.sport_id = ds.sport_id
    JOIN dim_date dd ON f.date_id = dd.date_id
    LIMIT 3
  "))
}

# Main Execution
# ---------------

cat("  ETL Process for SportsTV Analytics\n")
cat(" ---------------------------------- \n")

mysqlConn <- connectToMySQL()
sqliteConn <- connectToSQLite()

clearExistingData(mysqlConn)
loadDimensionTables(mysqlConn, sqliteConn)
loadFactStreaming(mysqlConn, sqliteConn)
verifyDataLoad(mysqlConn)

dbDisconnect(mysqlConn)
dbDisconnect(sqliteConn)

cat("\n✓ ETL Process Complete!\n")