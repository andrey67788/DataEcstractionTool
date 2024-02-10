# Instructions for using the Extraction Script
    
## This script is designed to connect to a SQL databases like PostgreSQL MySQL SQLite containing. 
## It performs data extraction from tables and saves the extracted data to CSV files If it is pass through th test.)
## Pre-requisites:
- Ensure you have Python installed on your system.
- Install the required libraries you can check in 'requirments.txt'
- Create a SQL database/or bases and populate it with the necessary tables.
    
## Usage:
1. Set up the script:
- Open the script in a Python environment or text editor.
- Modify the database connection details (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT) in the `DataPSQL` class to match 
your SQL database configuration.
2. Run the script:
- use cron to add a scenario of script starting, like this:
30 8 * * 1-5 /home/andrey67788/PycharmProjects/DataEcstractionTool/.venv/bin/python /home/andrey67788/PycharmProjects/DataEcstractionTool/main.py
for more information about cron usage https://crontab.guru/
  ```
- or run it manually
       
3. Check logs and extracted files:
- After running the script, check the 'logs' directory for log files containing information about script execution.
- The extracted CSV files will be available in the 'extracted' directory.
