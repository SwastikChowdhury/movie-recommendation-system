from dateutil import parser
from datetime import datetime
import os
import re
import mysql.connector
from kafka import KafkaConsumer
import logging
import signal
import sys
import csv
import numpy as np
from dotenv import load_dotenv
load_dotenv()

# Configure logging for the script
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MySQL connection setup
def connect_db():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )
        logger.info("Connected to MySQL")
        return conn
    except mysql.connector.Error as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        sys.exit(1)

# Connect to MySQL database
mysql_conn = connect_db()

# Kafka consumer setup
consumer = KafkaConsumer(
    'movielog2',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Regular expressions to parse log entries
recommendation_pattern = re.compile(
    r'(?P<time>[^,]+),(?P<userid>\d+),recommendation request (?P<server>[^,]+), status (?P<status>\d+), result: (?P<recommendations>[^,]+), (?P<responsetime>.+)'
)
watch_pattern = re.compile(
    r'(?P<time>[^,]+),(?P<userid>\d+),GET /data/m/(?P<movieid>[^/]+)/(?P<minute>\d+)\.mpg'
)
rate_pattern = re.compile(
    r'(?P<time>[^,]+),(?P<userid>\d+),GET /rate/(?P<movieid>[^=]+)=(?P<rating>\d)'
)

# CSV file setup to store logs
csv_file = 'kafka_data_test.csv'
csv_fields = ['log_type', 'log_time', 'userid', 'movieid', 'minute', 'rating', 'server', 'status', 'recommendations', 'responsetime']

# If the CSV file doesn't exist, create it and write headers
if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()

# SQL query for checking if a record exists
select_query = """
SELECT id, minute, rating FROM kafka_data WHERE userid=%s AND movieid=%s AND LEFT(log_time, 10) = %s
"""

# SQL query for updating an existing record (merging minutes and ratings)
update_query = """
UPDATE kafka_logs SET
    minute = IF(%s IS NOT NULL, CONCAT_WS(',', minute, %s), minute),
    rating = IF(%s IS NOT NULL, CONCAT_WS(',', rating, %s), rating)
WHERE id = %s
"""

# SQL query for inserting a new record
insert_query = """
INSERT INTO kafka_data (log_type, log_time, userid, movieid, minute, rating, server, status, recommendations, responsetime)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Function to validate data before inserting
def validate_data(log_entry):
    log_type, log_time, userid, movieid, minute, rating, server, status, recommendations, responsetime = log_entry
    
    # Ensure log_time is a valid timestamp
    try:
        parser.parse(log_time)
    except ValueError:
        logger.error(f"Invalid log_time: {log_time}")
        return False
    
    # Ensure userid is a number and not None
    if not re.match(r'^[a-zA-Z0-9]+$', userid):
        logger.error(f"Invalid userid: {userid}")
        return False

    # Ensure minute is valid when log_type is 'watch'
    if log_type == 'watch':
        if minute is None or (not minute.isdigit() or int(minute) < 0):
            logger.error(f"Invalid minute for log_type 'watch': {minute}")
            return False
    
    # Ensure rating is either None or a valid integer between 0 and 5
    if log_type == 'rate':
        if rating and (not rating.isdigit() or int(rating) < 0 or int(rating) > 5):
            logger.error(f"Invalid rating: {rating}")
            return False

    # Check for any other required validation (e.g., movieid should not be empty)
    if log_type != 'recommendation':
        if not movieid:
            logger.error(f"Invalid movieid: {movieid}")
            return False

    # Ensure status is 200
    if log_type == 'recommendation':
        if status != '200':
            logger.error(f"Invalid status: {status}")
            return False
    
    return True

# Variables for drift detection
rating_history = []
minute_history = []
window_size = 100  # Sliding window size for monitoring
threshold = 3  # Z-score threshold for anomaly detection

# Function to detect data drift using sliding window and anomaly detection
def detect_drift(data_list, value, field_name):
    data_list.append(value)
    if len(data_list) > window_size:
        data_list.pop(0)

    if len(data_list) == window_size:
        mean = np.mean(data_list)
        std = np.std(data_list)
        if std > 0:
            z_score = (value - mean) / std
            if abs(z_score) > threshold:
                logger.warning(f"Potential data drift detected in {field_name}: Value {value}, Z-score {z_score}")

# Function to process a single Kafka message
def process_message(message):
    log_entry = None

    # Match recommendation logs
    match = recommendation_pattern.match(message)
    if match:
        logger.info("Processing recommendation request...")
        data = match.groupdict()
        log_entry = (
            "recommendation", data['time'], data['userid'], None, None, None, data['server'],
            data['status'], data['recommendations'], data['responsetime']
        )

    # Match movie watch logs
    match = watch_pattern.match(message)
    if match:
        logger.info("Processing movie watch log...")
        data = match.groupdict()
        log_entry = (
            "watch", data['time'], data['userid'], data['movieid'], data['minute'], None, None, None, None, None
        )
        detect_drift(minute_history, int(data['minute']), "minute")


    # Match movie rating logs
    match = rate_pattern.match(message)
    if match:
        logger.info("Processing movie rating log...")
        data = match.groupdict()
        log_entry = (
            "rating", data['time'], data['userid'], data['movieid'], None, data['rating'], None, None, None, None
        )
        detect_drift(rating_history, int(data['rating']), "rating")
    
    # Validate the log entry before returning
    if log_entry is not None and validate_data(log_entry):
        return log_entry
    else:
        logger.warning("Invalid log entry skipped.")
        return None

# Function to insert or update a record
def insert_or_update_record(cursor, log_entry):
    log_type, log_time, userid, movieid, minute, rating, server, status, recommendations, responsetime = log_entry
    
    # Check if the record already exists
    # cursor.execute(select_query, (userid, movieid))
    log_date = log_time[:10]
    cursor.execute(select_query, (userid, movieid, log_date)) # Include date in the query
    result = cursor.fetchone()
    logger.info(f"Query result for userid={userid}, movieid={movieid}, log_date={log_date}: {result}")

    if result:
        # If the record exists, merge minute and rating as lists
        record_id, existing_minute, existing_rating = result

        # Ensure no duplicates in minute and rating
        new_minute = minute if existing_minute is None else f"{existing_minute},{minute}"
        new_rating = rating if existing_rating is None else f"{existing_rating},{rating}"

        # Merge existing and new minute/rating values
        cursor.execute(update_query, (minute, minute, rating, rating, record_id))
        logger.info(f"Updated record for user {userid} and movie {movieid}.")
    else:
        # If the record does not exist, insert a new one
        cursor.execute(insert_query, log_entry)
        logger.info(f"Inserted new record for user {userid} and movie {movieid}.")

# Function to write the record to CSV
def write_to_csv(log_entry):
    # Convert tuple to dictionary
    log_type, log_time, userid, movieid, minute, rating, server, status, recommendations, responsetime = log_entry
    csv_row = {
        'log_type': log_type,
        'log_time': log_time,
        'userid': userid,
        'movieid': movieid,
        'minute': minute,
        'rating': rating,
        'server': server,
        'status': status,
        'recommendations': recommendations,
        'responsetime': responsetime
    }

    # Write to CSV file
    with open(csv_file, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writerow(csv_row)
        logger.info(f"Written log to CSV for user {userid} and movie {movieid}.")

# Main function to consume Kafka messages and process them in batches
def main():
    cursor = mysql_conn.cursor()
    batch_size = 100  # Process data in batches of 100 messages
    processed_count = 0  # Count the number of processed messages
    max_records = 10000000  # Limit to 10 million records

    try:
        for message in consumer:
            log_entry = process_message(message.value)
            if log_entry:
                logger.info(f"Processed log_entry: {log_entry}")
                insert_or_update_record(cursor, log_entry)
                write_to_csv(log_entry)  # Write each log to CSV
                processed_count += 1

            if processed_count >= batch_size:
                mysql_conn.commit()  # Commit the batch to the database
                processed_count = 0  # Reset the counter
                logger.info(f"Processed and committed {batch_size} records.")

            if processed_count >= max_records:
                logger.info(f"Processed {max_records} records, stopping...")
                break

        # Commit any remaining records
        mysql_conn.commit()

    except KeyboardInterrupt:
        logger.info("Received interrupt, closing connections...")
    finally:
        close_connections()

# Function to close the database connection
def close_connections():
    if mysql_conn:
        mysql_conn.close()
        logger.info("MySQL connection closed.")

# Signal handler for graceful shutdown
def shutdown(signal_num, frame):
    logger.info("Shutting down gracefully...")
    close_connections()
    sys.exit(0)

# Register signal handlers for shutdown
signal.signal(signal.SIGINT, shutdown) 
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":
    main()
