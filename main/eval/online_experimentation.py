from flask import Flask, request, make_response
import mysql.connector
from mysql.connector import OperationalError, PoolError
import ast  # Safely evaluate strings that look like lists
import time  # For measuring response time
import logging
from mysql.connector import pooling
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask application
app = Flask(__name__)

# Only create the connection pool if needed
connection_pool = None
if os.environ.get("RUNNING_TESTS") is None:  # Only initialize in non-test environments
    connection_pool = pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=10,
        pool_reset_session=True,
        host="localhost",
        user="mluser",
        password=os.environ.get('DB_PASSWORD'),
        database=os.environ.get('DB_NAME')
    )

def log_metrics_to_db(endpoint, method, response_time, status_code, model_name,user_id):
    try:
        with connection_pool.get_connection() as conn:
            cursor = conn.cursor()
            query = """
            INSERT INTO online_experimentation (endpoint, method, response_time, status_code, model_name,user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (endpoint, method, response_time, status_code, model_name, user_id))
            conn.commit()
            cursor.close()
    except Exception as e:
        logger.error(f"Error logging metrics to database: {e}")

def get_random_recommendations():
    try:
        with connection_pool.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            # Fetch a random entry from the user_recommendations table
            query = "SELECT recommendations FROM user_recommendations ORDER BY RAND() LIMIT 1"
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()

            if row:
                recommendations_str = row['recommendations']
                recommendations_list = ast.literal_eval(recommendations_str)
                return sorted(recommendations_list, reverse=True)[:20]
            else:
                return None
    except Exception as e:
        logger.error(f"Error fetching random recommendations: {e}")
        return None

# Load top 20 movies at startup
top_20_movies = get_random_recommendations()

@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend(user_id):
    start_time = time.time()
    try:
        # Get recommendations and the model name
        user_recommendations = get_recommendations(user_id)

        if user_recommendations:
            recommendations, model_name = user_recommendations
        else:
            if user_id % 2 == 0:   
                recommendations, model_name = None, "movie_recommender_1"
            else:
                recommendations, model_name = None, "movie_recommender_2"

        response_time = time.time() - start_time
        status_code = 200
        logger.info(f"Response time for user {user_id}: {response_time:.2f} seconds, Status code: {status_code}")

        # Log metrics to the online_experimentation table
        log_metrics_to_db('/recommend', 'GET', response_time, status_code, model_name,user_id)

        if recommendations:
            # Replace spaces with plus signs and join the list of movie titles into a comma-separated string
            recommendations_str = ','.join(map(lambda x: str(x).replace(' ', '+'), recommendations))
            return recommendations_str, 200
        else:
            # Format top 20 movie titles as a fallback, replacing spaces with plus signs
            formatted_top_movies = ','.join(map(lambda x: str(x).replace(' ', '+'), top_20_movies))
            return formatted_top_movies, 200
    except Exception as e:
        response_time = time.time() - start_time
        logger.error(f"Error handling request for user {user_id}: {e}")
        # Log the failed request with status code 400
        log_metrics_to_db('/recommend', 'GET', response_time, 400, "unknown",user_id)
        return "Bad Request", 400

def get_recommendations(user_id):
    try:
        with connection_pool.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)

            if user_id % 2 == 0:
                query = """
                SELECT top_20_recommendations, model_name FROM user_recommendations_1 WHERE user_id = %s
                """
            else:
                query = """
                SELECT top_20_recommendations, model_name FROM user_recommendations_2 WHERE user_id = %s
                """
            cursor.execute(query, (user_id,))
            row = cursor.fetchone()
            cursor.close()

            if row:
                recommendations_str = row['top_20_recommendations']
                model_name = row['model_name']
                recommendations_list = ast.literal_eval(recommendations_str)
                return [sorted(recommendations_list, reverse=True)[:20], model_name]
            else:
                return None
    except PoolError as e:
        logger.error(f"PoolError encountered: {e}. No available connections.")
        return None
    except OperationalError as e:
        logger.error(f"OperationalError encountered: {e}. Retrying...")
        return get_recommendations(user_id)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)