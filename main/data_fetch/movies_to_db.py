import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MySQL connection setup
def connect_db():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="mluser",
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )
        logger.info("Connected to MySQL")
        return conn
    except Error as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        sys.exit(1)

def insert_data_from_csv(csv_file):
    # Connect to the database
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Read the CSV file
        data = pd.read_csv(csv_file)
        logger.info(f"Read CSV file: {csv_file}")

        # Insert each row into the table
        for index, row in data.iterrows():
            sql = """
            INSERT INTO movies (movieId, title, genres)
            VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (row['movieId'], row['title'], row['genres']))

        # Commit the transaction
        conn.commit()
        logger.info("Data inserted successfully")

    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()

    finally:
        # Close the connection
        cursor.close()
        conn.close()
        logger.info("Database connection closed")

# Usage
if __name__ == "__main__":
    csv_file_path = 'main/data/movies.csv'
    logger.info(f"Starting data insertion from {csv_file_path}")
    insert_data_from_csv(csv_file_path)
    logger.info("Data insertion process completed")