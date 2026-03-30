# This is the configuration file for database connections
# 

# INSTRUCTIONS:
# ---------------
# Update DB credentials below and ensure MySQL settings allow connections. 
# All scripts automatically source this file.


# MySQL Configuration (Cloud Analytics Database)
DB_HOST <- "practicum-1-kanad-northeastern-218f.f.aivencloud.com"
DB_PORT <- 20149
DB_NAME <- "defaultdb"
DB_USER <- "avnadmin"
DB_PASSWORD <- "YOUR_PASSWORD_HERE"

# SQLite Configuration (Operational Database)
SQLITE_DB <- "data/subscribersDB.sqlitedb"

# CSV File Configuration
CSV_FILE <- "data/new-streaming-transactions-98732.csv"