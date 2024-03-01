import pandas as pd
import os
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector

CURRENT_DIR = Path(__file__).parent.resolve()
DATASET_DIR = CURRENT_DIR.parent / "Soal 1 - Data Transformation dan Analysis Case"
print(DATASET_DIR)

# Load environment variables from .env
load_dotenv()

# Access credential MySQL
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_database = os.getenv("DB_DATABASE")

db_config = {
    'host': db_host,
    'user': db_user,
    'password': db_password,
    'database': db_database,
    'autocommit': True
}


def create_table(conn):
    query = """
        CREATE TABLE IF NOT EXISTS ecommerce_dataset (
        fullVisitorId VARCHAR(255),
        channelGrouping VARCHAR(255),
        time INT,
        country VARCHAR(255),
        city VARCHAR(255),
        totalTransactionRevenue FLOAT,
        transactions FLOAT,
        timeOnSite FLOAT,
        pageviews FLOAT,
        sessionQualityDim FLOAT,
        date INT,
        visitId BIGINT,
        type VARCHAR(255),
        productRefundAmount FLOAT,
        productQuantity FLOAT,
        productPrice FLOAT,
        productRevenue FLOAT,
        productSKU VARCHAR(255),
        v2ProductName VARCHAR(255),
        v2ProductCategory VARCHAR(255),
        productVariant VARCHAR(255),
        currencyCode VARCHAR(10),
        itemQuantity FLOAT,
        itemRevenue FLOAT,
        transactionRevenue FLOAT,
        transactionId VARCHAR(255),
        pageTitle VARCHAR(255),
        searchKeyword VARCHAR(255),
        pagePathLevel1 VARCHAR(255),
        eCommerceAction_type INT,
        eCommerceAction_step INT,
        eCommerceAction_option FLOAT
        )
    """

    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    print("Table ecommerce_dataset has been created!")

def get_data(filename):
    file = DATASET_DIR / filename
    try:
        df = pd.read_csv(file)
    except FileNotFoundError:
        df = pd.DataFrame([])
        print("File is not found!")
    return df

def insert_records(conn, values):
    query = """
        INSERT INTO ecommerce_dataset (fullVisitorId, channelGrouping, time, country, city, totalTransactionRevenue,
            transactions, timeOnSite, pageviews, sessionQualityDim, date, visitId, type, productRefundAmount, productQuantity,
            productPrice, productRevenue, productSKU, v2ProductName, v2ProductCategory, productVariant, currencyCode,
            itemQuantity, itemRevenue, transactionRevenue, transactionId, pageTitle, searchKeyword, pagePathLevel1,
            eCommerceAction_type, eCommerceAction_step, eCommerceAction_option)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()
    cursor.executemany(query, values)
    cursor.close()
    print("Data has been loaded into ecommerce_dataset!")


def main():
    retries = 3
    while retries > 0:
        try:
            conn = mysql.connector.connect(**db_config)
            create_table(conn)
            df = get_data("ecommerce-session-bigquery.csv")
            values = [tuple([i if pd.notna(i) else None for i in row]) for row in df.values]
            insert_records(conn, values)
            break
        
        except mysql.connector.Error as e:
            retries -= 1
            if retries == 0:
                # Handling connection error
                print(f"Error connecting to the database: {e}")

        finally:
            if 'conn' in locals() and conn.is_connected():
                conn.close()

if __name__=="__main__":
    main()