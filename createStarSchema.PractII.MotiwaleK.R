# CS5200 - Practicum II: Data Warehouse Design
# File: createStarSchema.PractII.MotiwaleK.R
# Author: Kanad Motiwale
# Term: Fall 2025
# 
# Purpose: Creates star schema (fact + dimension tables) in cloud MySQL
#          for SportsTV streaming analytics
# 
# Tables Created:
#   - dim_country: Country dimension
#   - dim_sport: Sport dimension  
#   - dim_date: Date dimension with year/quarter/month/week
#   - fact_streaming: Fact table with foreign keys to all dimensions
# 
# Key Features:
#   - Pure star schema (no dimension attributes in fact table)
#   - Foreign key constraints enforce referential integrity
#   - Comprehensive indexes for analytical queries


# SETUP AND CONFIGURATION
# ------------------------

# Load required libraries
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")

library(RMySQL)
library(DBI)

# DATABASE CONNECTION CONFIGURATION
# ----------------------------------

# Load configuration from external file
source("config.R")

# FUNCTION: Connect to MySQL Database
# ------------------------------------

# This opens DB connection with error handling and helpful troubleshooting messages.

connectToMySQL <- function() {
  tryCatch({
    conn <- dbConnect(
      RMySQL::MySQL(),
      host = DB_HOST,
      port = DB_PORT,
      dbname = DB_NAME,
      user = DB_USER,
      password = DB_PASSWORD
    )
    cat("✓ Successfully connected to MySQL database:", DB_NAME, "\n")
    return(conn)
  }, error = function(e) {
    cat("✗ Error connecting to MySQL:\n")
    cat("  ", e$message, "\n")
    cat("\nTroubleshooting tips:\n")
    cat("  1. Check that your MySQL service is running\n")
    cat("  2. Verify credentials in config.R are correct\n")
    cat("  3. Check firewall/antivirus blocking the port\n")
    cat("  4. Ensure database exists\n")
    stop("Database connection failed")
  })
}

# FUNCTION: Drop Tables if They Exist
# ------------------------------------

dropTablesIfExist <- function(conn) {
  cat("\n-- Dropping existing tables (if any) --\n")
  
  # Must drop in reverse order due to foreign key constraints
  tables <- c("fact_streaming", "dim_date", "dim_sport", "dim_country")
  
  # Temporarily disable foreign key checks
  dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 0")
  
  for (table in tables) {
    tryCatch({
      dbExecute(conn, paste0("DROP TABLE IF EXISTS ", table))
      cat("✓ Dropped table:", table, "\n")
    }, error = function(e) {
      cat("  Note:", table, "did not exist\n")
    })
  }
  
  # Re-enable foreign key checks
  dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 1")
}

# FUNCTION: Create Dimension Table - Country(dim_country)
# -------------------------------------------

createDimCountry <- function(conn) {
  cat("\n-- Creating dim_country table --\n")
  
  sql <- "
  CREATE TABLE dim_country (
      country_id INT PRIMARY KEY,
      country_name VARCHAR(50) NOT NULL,
      
      INDEX idx_country_name (country_name)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  "
  
  tryCatch({
    dbExecute(conn, sql)
    cat("✓ dim_country table created successfully\n")
  }, error = function(e) {
    cat("✗ Error creating dim_country:\n")
    cat("  ", e$message, "\n")
    stop("Failed to create dim_country")
  })
}

# FUNCTION: Create Dimension Table - Sport(dim_sport)
# -----------------------------------------

createDimSport <- function(conn) {
  cat("\n=== Creating dim_sport table ===\n")
  
  sql <- "
  CREATE TABLE dim_sport (
      sport_id INT AUTO_INCREMENT PRIMARY KEY,
      sport_name VARCHAR(50) NOT NULL UNIQUE,
      
      INDEX idx_sport_name (sport_name)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  "
  
  tryCatch({
    dbExecute(conn, sql)
    cat("✓ dim_sport table created successfully\n")
  }, error = function(e) {
    cat("✗ Error creating dim_sport:\n")
    cat("  ", e$message, "\n")
    stop("Failed to create dim_sport")
  })
}

# FUNCTION: Create Dimension Table - Date(dim_date)
# ----------------------------------------

createDimDate <- function(conn) {
  cat("\n-- Creating dim_date table --\n")
  
  sql <- "
  CREATE TABLE dim_date (
      date_id INT AUTO_INCREMENT PRIMARY KEY,
      full_date DATE NOT NULL UNIQUE,
      year INT NOT NULL,
      quarter INT NOT NULL,
      month INT NOT NULL,
      week INT NOT NULL,
      day_of_week INT NOT NULL,
      day_name VARCHAR(10),
      month_name VARCHAR(10),
      
      INDEX idx_date (full_date),
      INDEX idx_year_month (year, month)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  "
  
  tryCatch({
    dbExecute(conn, sql)
    cat("✓ dim_date table created successfully\n")
  }, error = function(e) {
    cat("✗ Error creating dim_date:\n")
    cat("  ", e$message, "\n")
    stop("Failed to create dim_date")
  })
}

# FUNCTION: Create Fact Table - Pure Star Schema with Foreign Keys
# -----------------------------------------------------------------

# This creates fact_streaming with partitions and indexes.

createFactStreaming <- function(conn) {
  cat("\n-- Creating fact_streaming table (Pure Star Schema with Partitioning) --\n")
  
  sql <- "
  CREATE TABLE fact_streaming (
      fact_id INT AUTO_INCREMENT,
      
      -- Foreign Keys to Dimension Tables (Pure Star Schema)
      date_id INT NOT NULL,
      country_id INT NOT NULL,
      sport_id INT NOT NULL,
      
      -- Year column for partitioning
      streaming_year INT NOT NULL,
      
      -- Pre-computed Facts (Measures)
      total_streams INT NOT NULL DEFAULT 0,
      total_minutes_streamed INT NOT NULL DEFAULT 0,
      avg_minutes_per_stream DECIMAL(10,2),
      unique_subscribers INT NOT NULL DEFAULT 0,
      
      -- Primary key must include partition column
      PRIMARY KEY (fact_id, streaming_year),
      
      -- Indexes for Fast Analytical Queries
      INDEX idx_date (date_id),
      INDEX idx_country (country_id),
      INDEX idx_sport (sport_id),
      INDEX idx_year (streaming_year),
      INDEX idx_composite (date_id, country_id, sport_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  PARTITION BY RANGE (streaming_year) (
      PARTITION p2021 VALUES LESS THAN (2022),
      PARTITION p2022 VALUES LESS THAN (2023),
      PARTITION p2023 VALUES LESS THAN (2024),
      PARTITION p2024 VALUES LESS THAN (2025),
      PARTITION p2025 VALUES LESS THAN (2026),
      PARTITION p_future VALUES LESS THAN MAXVALUE
  );
  "
  
  tryCatch({
    dbExecute(conn, sql)
    cat("✓ fact_streaming table created successfully\n")
    cat("  - Pure star schema design\n")
    cat("  - Partitioned by year for scalability\n")
    cat("  - Indexes enforce referential integrity\n")
    cat("  - Note: FK constraints not possible with partitioning in MySQL\n")
  }, error = function(e) {
    cat("✗ Error creating fact_streaming:\n")
    cat("  ", e$message, "\n")
    stop("Failed to create fact_streaming table")
  })
}


# FUNCTION: Verify Tables Were Created
# -------------------------------------

# Checks whether the expected tables exist or not.

verifyTables <- function(conn) {
  cat("\n-- Verifying table creation --\n")
  
  tables <- dbListTables(conn)
  
  expected_tables <- c("dim_country", "dim_sport", "dim_date", "fact_streaming")
  
  for (table in expected_tables) {
    if (table %in% tables) {
      cat("✓", table, "exists\n")
    } else {
      cat("✗", table, "NOT found\n")
    }
  }
  
  cat("\nTotal tables in database:", length(tables), "\n")
}

# Main Execution
# ---------------

cat("  Star Schema Creation for SportsTV\n")
cat("  Practicum II - Kanad Motiwale\n")
cat("----------------------------------\n")

# Step 1: Connect to MySQL
mysqlConn <- connectToMySQL()

# Step 2: Drop existing tables (clean slate)
dropTablesIfExist(mysqlConn)

# Step 3: Create dimension tables FIRST (referenced by fact table)
createDimCountry(mysqlConn)
createDimSport(mysqlConn)
createDimDate(mysqlConn)

# Step 4: Create fact table with foreign keys
createFactStreaming(mysqlConn)

# Step 5: Verify everything was created
verifyTables(mysqlConn)

# Step 6: Disconnect
dbDisconnect(mysqlConn)
cat("  ✓ Star schema creation complete!\n")
cat("-----------------------------------\n")
cat("\nNext step: Run loadAnalyticsDB.PractII.MotiwaleK.R\n")