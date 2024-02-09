import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Create base class for declarative ORM
Base = declarative_base()


# Define the Customer class to represent the customers table
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String)


# Define the Product class to represent the products table
class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    type = Column(String)
    price = Column(Float)


# Define the Purchase class to represent the purchases table
class Purchase(Base):
    __tablename__ = 'purchases'

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.id'))
    product_id = Column(Integer, ForeignKey('products.id'))
    purchase_date = Column(Date)
    quantity = Column(Integer)
    cpc = Column(Float)

    # Define the relationship with the Customer and Product tables
    customer = relationship('Customer', backref='purchases')
    product = relationship('Product', backref='purchases')


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

    def extraction(self):
        if not self.session:
            return

        try:
            # Fetch data from the customers table
            customers_query_result = self.session.query(Customer.id, Customer.name).filter(Customer.id.isnot(None)).all()
            customers_df = pd.DataFrame(customers_query_result, columns=['id', 'name'])
            customers_csv_file_path = os.path.join(os.path.abspath('.'), 'extracted', 'customers.csv')
            customers_df.to_csv(customers_csv_file_path, index=False)
            logs_instance.write_to_log(f'Data from customers table extracted and saved to {customers_csv_file_path}')

            # Fetch data from the products table
            products_query_result = self.session.query(Product.id, Product.type, Product.price).filter(
                Product.id.isnot(None)).all()
            products_df = pd.DataFrame(products_query_result, columns=['id', 'name', 'price'])
            products_csv_file_path = os.path.join(os.path.abspath('.'), 'extracted', 'products.csv')
            products_df.to_csv(products_csv_file_path, index=False)
            logs_instance.write_to_log(f'Data from products table extracted and saved to {products_csv_file_path}')

            # Fetch data from the purchases table
            purchases_query_result = self.session.query(Purchase.id, Purchase.customer_id, Purchase.product_id,
                                                        Purchase.purchase_date, Purchase.quantity,
                                                        Purchase.cpc).filter(
                Purchase.id.isnot(None)).all()
            purchases_df = pd.DataFrame(purchases_query_result,
                                        columns=['id', 'customer_id', 'product_id', 'purchase_date', 'quantity',
                                                 'total_price'])
            purchases_csv_file_path = os.path.join(os.path.abspath('.'), 'extracted', 'purchases.csv')
            purchases_df.to_csv(purchases_csv_file_path, index=False)
            logs_instance.write_to_log(f'Data from purchases table extracted and saved to {purchases_csv_file_path}')

        except Exception as e:
            logs_instance.write_to_log(f'Error occurred during data extraction: {e}')

        finally:
            if self.session:
                self.session.close()

class DataIntegrityChecker:
    def __init__(self, session):
        self.session = session

    def check_data_integrity(self):
        if not self.session:
            return False

        try:
            # Check integrity for the customers table
            customers_rows_count = self.session.query(Customer).count()
            customers_null_count = self.session.query(Customer).filter(
                (Customer.id.is_(None)) | (Customer.name.is_(None))
            ).count()
            customers_null_percentage = customers_null_count / customers_rows_count

            # Check integrity for the products table
            products_rows_count = self.session.query(Product).count()
            products_null_count = self.session.query(Product).filter(
                (Product.id.is_(None)) | (Product.type.is_(None)) | (Product.price.is_(None))
            ).count()
            products_null_percentage = products_null_count / products_rows_count

            # Check integrity for the purchases table
            purchases_rows_count = self.session.query(Purchase).count()
            purchases_null_count = self.session.query(Purchase).filter(
                (Purchase.id.is_(None)) | (Purchase.customer_id.is_(None)) | (Purchase.product_id.is_(None)) |
                (Purchase.purchase_date.is_(None)) | (Purchase.quantity.is_(None)) | (Purchase.cpc.is_(None))
            ).count()
            purchases_null_percentage = purchases_null_count / purchases_rows_count

            # Log the results
            logs_instance.write_to_log(
                f"Data integrity check completed for customers: {customers_rows_count} rows found, "
                f"{customers_null_count} null values found. Null percentage: {customers_null_percentage:.2f}"
            )
            logs_instance.write_to_log(
                f"Data integrity check completed for products: {products_rows_count} rows found, "
                f"{products_null_count} null values found. Null percentage: {products_null_percentage:.2f}"
            )
            logs_instance.write_to_log(
                f"Data integrity check completed for purchases: {purchases_rows_count} rows found, "
                f"{purchases_null_count} null values found. Null percentage: {purchases_null_percentage:.2f}"
            )

            # Check if any null percentage is more than 80%
            if customers_null_percentage > 0.8 or products_null_percentage > 0.8 or purchases_null_percentage > 0.8:
                logs_instance.write_to_log('Error: More than 80% NULL values found in one or more tables.')
                return False

        except Exception as e:
            logs_instance.write_to_log(f'Error during data integrity check: {e}')
            return False

        return True


# logs file Initial
logs_instance = Logs()

# Establish PostgreSQL connection
psql_connection = DataPSQL()
if psql_connection.connect():
    # If connection successful, perform data integrity check
    data_integrity_checker = DataIntegrityChecker(psql_connection.session)
    if not data_integrity_checker.check_data_integrity():
        logs_instance.write_to_log('Data integrity check failed. Aborting extraction.')
    else:
        psql_connection.extraction()
