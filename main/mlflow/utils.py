import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from tqdm import tqdm

def connect_db():
    """Establish database connection using environment variables."""
    load_dotenv(dotenv_path='../.env')
    return mysql.connector.connect(
        host="17645-team02.isri.cmu.edu",
        port=3306,
        user=os.environ.get('USERNAME'),
        password=os.environ.get('DB_PASSWORD'),
        database=os.environ.get('DB_NAME')
    )

def load_and_preprocess_data():
    """Load data from SQL database and preprocess it for model training."""
    # Connect to the database
    mysql_conn = connect_db()
    
    # Load data from SQL
    data_version = 2  # Explicitly track the version
    query = f"SELECT * FROM silver_movie_data WHERE version = {data_version}"
    count_query = f"SELECT COUNT(*) FROM silver_movie_data WHERE version = {data_version}"
    count_result = pd.read_sql(count_query, mysql_conn)
    total_records = count_result.iloc[0,0]
    print(f"Number of records to load: {total_records}")
    data = pd.read_sql(query, mysql_conn)
    mysql_conn.close()

    # Create data version info dictionary
    data_version_info = {
        'data_version': data_version,
        'total_records': total_records,
        'initial_features': list(data.columns),
        'data_source': 'silver_movie_data'
    }

    print("Data loaded from SQL")
    
    # Preprocess data
    data = data[data['user_rating'] != -1]
    print("Data columns:", data.columns)
    
    label_encoder_movie = LabelEncoder()
    data['movie_name_encoded'] = label_encoder_movie.fit_transform(data['movie_name'])

    ## TODO: Remove log_date and log_timestamp from the features for chronological splitting 
    X = data.drop(['user_rating', 'movie_name', 'tmdb_rating', 'log_date', 'log_timestamp'], axis=1)
    y = data['user_rating']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test, label_encoder_movie, data_version_info

def save_recommendations(user_recommendations, model_name):
    """
    Save user recommendations to the database.
    
    Args:
        user_recommendations (dict): Dictionary with user_id as key and list of recommendations as value
        model_name (str): Name of the model used for recommendations
    """
    mysql_conn = connect_db()
    cursor = mysql_conn.cursor()

    # Create the recommendations table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_recommendations_test (
        user_id INT,
        model_name VARCHAR(255),
        top_20_recommendations TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, model_name)
    )
    """
    cursor.execute(create_table_query)
    mysql_conn.commit()
    
    try:
        for user_id, movies in tqdm(user_recommendations.items(), desc="Saving recommendations"):
            # Convert numpy.int64 to regular Python int
            user_id = int(user_id)
            movies_str = ','.join(movies)
            
            query = """
                INSERT INTO user_recommendations_test (user_id, model_name, top_20_recommendations)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                top_20_recommendations = VALUES(top_20_recommendations),
                created_at = CURRENT_TIMESTAMP
            """
            cursor.execute(query, (user_id, model_name, movies_str))
        
        mysql_conn.commit()
        print(f"Successfully saved recommendations for {len(user_recommendations)} users")
        
    except Exception as e:
        print(f"Error saving recommendations: {str(e)}")
        mysql_conn.rollback()
    
    finally:
        cursor.close()
        mysql_conn.close()