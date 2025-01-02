import pandas as pd
import re
from typing import Dict, List
import logging
from functools import partial
import mysql.connector
from mysql.connector import Error
import sys
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

def load_data_from_mysql(conn, table_name: str) -> pd.DataFrame:
    """Load data from a MySQL table."""
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        logger.info(f"Data loaded from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {table_name}: {e}")
        raise
    
def format_movie_name(movieid: str) -> str:
    """Format movieid in kafka_data."""
    movieid = movieid.replace('+', ' ').replace('_', '')
    return re.sub(r'(\d{4})$', r'(\1)', movieid)

def clean_text(text: str) -> str:
    """Remove punctuation and convert to lowercase."""
    return re.sub(r'[^\w\s]', '', text).strip().lower()


def clean_tmdb_data(tmdb_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess TMDB data."""
    df = tmdb_df.copy()
    df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
    df = df.dropna(subset=['year'])
    df['year'] = df['year'].astype(int)
    df['movie_name'] = (df['title'].apply(lambda x: clean_text(format_movie_name(x))) + ' ' + df['year'].astype(str))
    df['tmdb_rating'] = df['vote_average'] / 2
    return df[['movie_name', 'tmdb_rating']]


def process_movies_data(movies_df: pd.DataFrame) -> pd.DataFrame:
    """Process movies data."""
    genres_one_hot = movies_df['genres'].str.get_dummies(sep='|')
    movies_df = pd.concat([movies_df, genres_one_hot], axis=1)
    movies_df['movie_name'] = movies_df['title'].apply(lambda x: clean_text(format_movie_name(x)))
    movies_df.drop(columns=['genres', 'movieId', 'title'], inplace=True)
    movies_df.rename(columns={'(no genres listed)': 'no_genre'}, inplace=True)
    return movies_df



def process_kafka_data(kafka_df: pd.DataFrame) -> tuple:
    """Process kafka data."""
    kafka_ratings = kafka_df[kafka_df['log_type'] != 'recommendation'].copy()
    kafka_ratings.loc[:, 'movie_name'] = kafka_ratings['movieid'].apply(lambda x: clean_text(format_movie_name(x)))
    kafka_ratings = kafka_ratings[['movie_name', 'userid', 'rating']]
    kafka_ratings.columns = ['movie_name', 'user_id', 'user_rating']

    kafka_watch = kafka_df[kafka_df['log_type'] == 'watch'].copy()
    kafka_watch.loc[:, 'movie_name'] = kafka_watch['movieid'].apply(lambda x: clean_text(format_movie_name(x)))
    kafka_watch['minute'] = kafka_watch['minute'].str.split(',').str[-1].astype(float)
    kafka_watch = kafka_watch.rename(columns={'userid': 'user_id'})
    kafka_watch = kafka_watch[['user_id', 'movie_name', 'minute']]
    
    return kafka_ratings, kafka_watch


def create_table(conn):
    """Create the silver_movie_data table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS silver_movie_data (
        movie_name VARCHAR(255),
        user_id INT,
        no_genre INT,
        Action INT,
        Adventure INT,
        Animation INT,
        Children INT,
        Comedy INT,
        Crime INT,
        Documentary INT,
        Drama INT,
        Fantasy INT,
        Film_Noir INT,
        Horror INT,
        IMAX INT,
        Musical INT,
        Mystery INT,
        Romance INT,
        Sci_Fi INT,
        Thriller INT,
        War INT,
        Western INT,
        user_rating DECIMAL(2, 1),
        tmdb_rating DECIMAL(2, 1),
        minute DECIMAL(8, 1),
        release_year INT,
        PRIMARY KEY (movie_name, user_id)
    )
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Table 'silver_movie_data' created successfully")
    except Error as e:
        logger.error(f"Error creating table: {e}")
    finally:
        cursor.close()

def write_to_mysql(conn, df: pd.DataFrame, table_name: str, batch_size=50000):
    """
    Write DataFrame to MySQL table in batches
    
    Args:
        conn: MySQL connection
        df: DataFrame to write
        table_name: Target table name
        batch_size: Number of rows to insert in each batch
    """
    try:
        # Replace NaN values with None
        df = df.where(pd.notnull(df), None)
        
        # Get total number of rows
        total_rows = len(df)
        print(total_rows)
        
        # Process in batches
        for i in range(0, total_rows, batch_size):
            cursor = conn.cursor()
            
            # Get the batch
            batch_df = df.iloc[i:i + batch_size]
            
            # Create a list of tuples from the batch
            tuples = [tuple(x) for x in batch_df.to_numpy()]
            
            # Comma-separated dataframe columns
            cols = ','.join(list(df.columns))
            
            # SQL query to execute
            query = f"INSERT INTO {table_name} ({cols}) VALUES ({','.join(['%s' for _ in df.columns])}) ON DUPLICATE KEY UPDATE "
            query += ", ".join([f"{col} = VALUES({col})" for col in df.columns if col not in ('movie_name', 'user_id')])
            
            try:
                cursor.executemany(query, tuples)
                conn.commit()
                logger.info(f"Batch {i//batch_size + 1} written successfully ({i} to {min(i+batch_size, total_rows)} rows)")
            except Error as e:
                logger.error(f"Error writing batch {i//batch_size + 1}: {e}")
                conn.rollback()
            finally:
                cursor.close()
                
        logger.info(f"All data successfully written to {table_name}")
        
    except Error as e:
        logger.error(f"Error in write_to_mysql: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in write_to_mysql: {e}")
        
def extract_year(movie_name):
    try:
        year = int(movie_name[-4:])
        return year
    # TODO : Some movie like vermont is for lover no year for kafka hence issue!
    except (ValueError, TypeError):
        return 0

def main():
    # Connect to MySQL
    conn = connect_db()

    # Create table if it doesn't exist
    create_table(conn)

    # Load data from MySQL
    movies_df = load_data_from_mysql(conn, 'movies')
    kafka_df = load_data_from_mysql(conn, 'kafka_data')
    tmdb_df = load_data_from_mysql(conn, 'tmdb')
    
    # Process data
    movies_processed = process_movies_data(movies_df)
    kafka_ratings, kafka_time = process_kafka_data(kafka_df)
    tmdb_cleaned = clean_tmdb_data(tmdb_df)
    

    # Merge dataframes
    merged_df = kafka_ratings.merge(tmdb_cleaned, on='movie_name', how='left')
    merged_df = merged_df.merge(movies_processed, on='movie_name', how='left')
    final_df = merged_df.merge(kafka_time, on=['user_id', 'movie_name'], how='left')
    final_df['release_year'] = final_df['movie_name'].apply(extract_year).astype(int)
    final_df = final_df.rename(columns={'Film-Noir': 'Film_Noir','Sci-Fi': 'Sci_Fi'})

    # Reorder columns to match the table structure
    columns_order = ['movie_name', 'user_id', 'no_genre', 'Action', 'Adventure', 'Animation', 'Children', 
                     'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film_Noir', 'Horror', 'IMAX', 
                     'Musical', 'Mystery', 'Romance', 'Sci_Fi', 'Thriller', 'War', 'Western', 
                     'user_rating', 'tmdb_rating', 'minute', 'release_year']
    
    final_df = final_df[columns_order]
    
    # Convert data types to match MySQL table
    final_df['user_id'] = final_df['user_id'].astype(int)
    final_df['user_rating'] = pd.to_numeric(final_df['user_rating'], errors='coerce')
    final_df['tmdb_rating'] = pd.to_numeric(final_df['tmdb_rating'], errors='coerce')
    final_df['minute'] = pd.to_numeric(final_df['minute'], errors='coerce')
    final_df['release_year'] = final_df['release_year'].astype(int)
    final_df = final_df.fillna(0)
     
    # Write to MySQL
    write_to_mysql(conn, final_df, 'silver_movie_data')

    # Close MySQL connection
    conn.close()

    logger.info(f"Data processing completed. Results saved to table : silver_movie_data.")

if __name__ == "__main__":
    main()