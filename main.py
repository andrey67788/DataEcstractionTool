import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create base class for declarative ORM
Base = declarative_base()

# Define the Customer class to represent the customers table
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String)

class Logs:
    log_dir = os.path.abspath(os.path.join('.', 'logs'))
    message = 'Initial completed'

    def __init__(self):
        self.log_file = None
        self.open_log_file()

    def generate_log_filename(self):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        logfile = os.path.join(self.log_dir, f'logfile_{timestamp}.log')
        return logfile

    def open_log_file(self):
        self.log_file = self.generate_log_filename()
        mode = 'w' if not os.path.exists(self.log_file) else 'a'
        self.log_handler = open(self.log_file, mode)

    def write_to_log(self, message):
        log_entry = f'[{datetime.datetime.now()}] {message}\n'
        self.log_handler.write(log_entry)
        self.log_handler.flush()


class DataPSQL:
    DB_USER = 'andrey67788'
    DB_PASSWORD = 'admin'
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'insurance_company'

    def __init__(self):
        self.engine = None
        self.session = None

    def connect(self):
        try:
            logs_instance = Logs()

            # Connect to the PostgreSQL database using SQLAlchemy
            db_url = f'postgresql://{DataPSQL.DB_USER}:{DataPSQL.DB_PASSWORD}@{DataPSQL.DB_HOST}:{DataPSQL.DB_PORT}/{DataPSQL.DB_NAME}'
            self.engine = create_engine(db_url)

            # Create tables if not exist and bind engine to the base
            Base.metadata.create_all(self.engine)

            # Create a session
            Session = sessionmaker(bind=self.engine)
            self.session = Session()

            logs_instance.write_to_log(f"PSQL database '{DataPSQL.DB_NAME}' connected")
            return True

        except Exception as e:
            logs_instance.write_to_log(f'Alert: Failed to connect to PSQL database: {e}')
            return False

    def check_data_integrity(self):
        if not self.session:
            return False

        try:
            print()
            # Count the number of rows in the customers table
            rows_count = self.session.query(Customer).count()
            print(rows_count)

            # Check for null values in id and name columns
            null_count = self.session.query(Customer).filter(
                (Customer.id.is_(None)) | (Customer.name.is_(None))
            ).count()

            # Calculate the percentage of null values
            null_percentage = null_count / rows_count

            if null_percentage > 0.8:
                # If the percentage of null values is more than 80%, log an error
                logs_instance.write_to_log('Error: More than 80% NULL values found in the column.')
                return False
            #
            logs_instance.write_to_log(
                f"Data integrity check completed: {rows_count} rows found, {null_count} null values found."
            )

        except Exception as e:
            logs_instance.write_to_log(f'Error during data integrity check: {e}')
            return False

        return True

    def extraction(self):
        if not self.session:
            return

        try:
            # Fetch data from the customers table
            query_result = self.session.query(Customer.id, Customer.name).filter(Customer.id.isnot(None)).all()

            # Convert query result to DataFrame
            df = pd.DataFrame(query_result, columns=['id', 'name'])

            # Define directory and file path for saving extracted data
            extracted_dir = os.path.join(os.path.abspath('.'), 'extracted')
            os.makedirs(extracted_dir, exist_ok=True)
            csv_file_path = os.path.join(extracted_dir, 'PSQL_data.csv')

            # Save DataFrame to CSV
            df.to_csv(csv_file_path, index=False)

            logs_instance.write_to_log(f'Data from PSQL extracted and saved to {csv_file_path}')

        except Exception as e:
            logs_instance.write_to_log(f'Error occurred during data extraction: {e}')

        finally:
            # Close the session
            if self.session:
                self.session.close()


# logs file Initial
logs_instance = Logs()

# Establish PostgreSQL connection
psql_connection = DataPSQL()
if psql_connection.connect():
    # If connection successful, perform data extraction
    if not psql_connection.check_data_integrity():
        logs_instance.write_to_log('Data integrity check failed. Aborting extraction.')
    else:
        psql_connection.extraction()

