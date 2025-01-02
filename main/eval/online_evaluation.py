import os
import mysql.connector
from mysql.connector import Error, pooling
import time
import logging

# Set up telemetry logging
logging.basicConfig(filename='telemetry.log', level=logging.INFO)

def connect_to_database():
    try:
        connection_pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=10,
            pool_reset_session=True,
            host="localhost",
            user="mluser",
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )
        print("Connection pool created successfully.")
        return connection_pool
    except Error as e:
        print(f"Error: {e}")
        return None

def fetch_user_recommendations(connection_pool):
    connection = connection_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT user_id, recommendations FROM user_recommendations")
    recommendations = cursor.fetchall()
    
    rec_dict = {}
    for row in recommendations:
        user_id = row['user_id']
        #print(type(user_id))
        raw_recommendations = row['recommendations']
        
        # Debug print for each raw entry
        #print(f"Raw recommendations for user {user_id}: {raw_recommendations}")
        
        # Split by ', ' but retain titles with numbers intact and remove empty strings
        movie_list = [normalize_title(movie) for movie in raw_recommendations.strip("[]").replace("'", "").split(", ") if movie]
        
        # Filter out any empty strings resulting from parsing
        rec_dict[str(user_id)] = {movie for movie in movie_list if movie}
        
        # Debug print for parsed set
        #print(f"Parsed recommendations for user {user_id}: {rec_dict[user_id]}")
    
    print(len(recommendations))
    cursor.close()
    connection.close()
    return rec_dict

def normalize_title(title):
    """
    Convert title to a standardized format:
    - Replace '+' with spaces.
    - Remove any trailing year.
    """
    # Replace '+' with spaces
    title = title.replace(' ', '+')
    # Remove trailing year (four-digit number at the end)
    title = ' '.join(word for word in title.split() if not word.isdigit())
    return title.lower().strip()

def evaluate_recommendation_usage(connection_pool, recommendations):
    connection = connection_pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    #cursor.execute("SELECT userid, movieid FROM kafka_data WHERE log_type = 'watch'")
    cursor.execute("SELECT userid, movieid FROM kafka_data WHERE log_type = 'watch'")
    watch_data = cursor.fetchall()

    matched_count = 0
    total_count = 0
    results = []

    # Normalize recommendations movie titles for easier matching
    normalized_recommendations = {user_id: {normalize_title(movie) for movie in rec_movies}
                                  for user_id, rec_movies in recommendations.items()}
        
    #print("NORMALIZED", normalized_recommendations)

    print(len(watch_data))

    for row in watch_data:
        user_id = row['userid']
        #print(type(user_id))
        movie_id = normalize_title(row['movieid'])  # Normalize watched movie title
        
        # Debugging output
        #print(f"Normalized watched movie for user {user_id}: {movie_id}")
        #print(f"Normalized recommendations for user {user_id}: {normalized_recommendations.get(user_id, set())}")
        #print("USER, MOVIE", user_id, movie_id)

        # Check if the normalized movie_id is in the normalized recommendations for the user
        if user_id in normalized_recommendations and movie_id in normalized_recommendations[user_id]:
            #print("USER11, MOVIE11", user_id, movie_id)
            matched_count += 1
            results.append((user_id, movie_id, True))  # Watched a recommended movie
        else:
            results.append((user_id, movie_id, False))  # Did not watch a recommended movie

        total_count += 1

    cursor.close()
    connection.close()

    # Calculate Match Rate
    match_rate = (matched_count / total_count ) * 100 if total_count > 0 else 0
    logging.info(f"Match Rate: {match_rate:.2f}% ({matched_count}/{total_count})")

    return results, match_rate

def store_evaluation_results(connection_pool, results):
    connection = connection_pool.get_connection()
    cursor = connection.cursor()
    for user_id, movie_id, matched in results:
        cursor.execute("""
            INSERT INTO recommendation_evaluation (user_id, movie_id, matched)
            VALUES (%s, %s, %s)
        """, (user_id, movie_id, matched))
    connection.commit()
    cursor.close()
    connection.close()
    print("Evaluation results stored in recommendation_evaluation table.")

def main():
    # Step 1: Connect to the database
    connection_pool = connect_to_database()
    if connection_pool is None:
        print("Failed to create the connection pool. Exiting.")
        return
    # Step 2: Fetch user recommendations
    recommendations = fetch_user_recommendations(connection_pool)

    # Step 3: Evaluate recommendation usage and log match rate
    start_time = time.time()
    results, match_rate = evaluate_recommendation_usage(connection_pool, recommendations)
    response_time = time.time() - start_time
    logging.info(f"Evaluation response time: {response_time:.4f} seconds")

    # Step 4: Store the results in a new table
    store_evaluation_results(connection_pool, results)

    print("Sample recommendations:", list(recommendations.items())[:5])

    print("Script completed successfully.")
    print(f"Match Rate: {match_rate:.2f}%")

if __name__ == "__main__":
    main()
