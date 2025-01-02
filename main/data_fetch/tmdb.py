import asyncio
import aiohttp
import os
import mysql.connector
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_movies(session, url, params):
    async with session.get(url, params=params) as response:
        if response.status == 200:
            return await response.json()
        else:
            logger.error(f"Error: {response.status}")
            return None

async def get_movies_by_date_range(session, start_date, end_date, api_key, cursor):
    base_url = 'https://api.themoviedb.org/3/discover/movie'
    page = 1
    total_pages = float('inf')
    movie_count = 0

    while page <= total_pages and page <= 500:
        params = {
            'api_key': api_key,
            'language': 'en-US',
            'sort_by': 'popularity.desc',
            'include_adult': 'false',
            'include_video': 'false',
            'page': page,
            'primary_release_date.gte': start_date,
            'primary_release_date.lte': end_date
        }

        data = await fetch_movies(session, base_url, params)
        if data:
            movies = data['results']
            total_pages = min(data['total_pages'], 500)

            for movie in movies:
                title = movie['title']
                release_date = movie['release_date']
                vote_average = movie['vote_average']
                overview = movie['overview'][:100]  # Truncate overview to 100 characters

                # Insert movie into the database
                try:
                    cursor.execute("""
                        INSERT INTO tmdb (title, release_date, vote_average, overview)
                        VALUES (%s, %s, %s, %s)
                    """, (title, release_date, vote_average, overview))
                except mysql.connector.Error as err:
                    logger.error(f"Error inserting movie into database: {err}")

                movie_count += 1

            logger.info(f"Processed page {page} of {total_pages} for date range {start_date} to {end_date}. Total movies: {movie_count}")
            page += 1
        else:
            break

    return movie_count

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
    except mysql.connector.Error as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        raise

async def get_all_movies():
    api_key = os.environ.get('TMDB_API_KEY')
    if not api_key:
        raise ValueError("TMDB_API_KEY environment variable is not set")

    start_date = datetime(1900, 1, 1)
    # end_date = datetime.now() -------- TODO TESTING AIR FLOW
    end_date = datetime(1900,2,1)
    interval = timedelta(days=365)  # 1 year interval
    total_movies = 0

    mysql_conn = connect_db()
    cursor = mysql_conn.cursor()

    # Create the movies table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tmdb (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            release_date DATE,
            vote_average FLOAT,
            overview TEXT
        )
    """)

    async with aiohttp.ClientSession() as session:
        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + interval, end_date)
            movies_count = await get_movies_by_date_range(
                session,
                current_start.strftime('%Y-%m-%d'),
                current_end.strftime('%Y-%m-%d'),
                api_key,
                cursor
            )
            total_movies += movies_count
            current_start = current_end + timedelta(days=1)
            mysql_conn.commit()

    logger.info(f"Total movies fetched and saved to database: {total_movies}")
    cursor.close()
    mysql_conn.close()

async def main():
    await get_all_movies()

if __name__ == "__main__":
    asyncio.run(main())