import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

import mysql.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
import numpy as np
import time
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import csv
from tqdm import tqdm
import ray
import argparse
import joblib
import os

def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="mluser",
        password=os.environ.get('DB_PASSWORD'),
        database=os.environ.get('DB_NAME')
    )

def load_and_preprocess_data():
    # Connect to the database
    mysql_conn = connect_db()
    
    # Load data from SQL
    query = "SELECT * FROM silver_movie_data"
    data = pd.read_sql(query, mysql_conn)
    
    # Close the database connection
    mysql_conn.close()

    print("Data loaded from SQL")
    # Preprocess data
    data = data[data['user_rating'] != 0]
    print("data columns: ", data.columns)
    label_encoder_movie = LabelEncoder()
    data['movie_name_encoded'] = label_encoder_movie.fit_transform(data['movie_name'])

    X = data.drop(['user_rating', 'movie_name','tmdb_rating'], axis=1)

    y = data['user_rating']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    return X_train, X_test, y_train, y_test, label_encoder_movie

def grid_search_and_save_model(X_train, X_test, y_train, y_test, model_name):
    start_time = time.time()
    # Create the pipeline
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('rf', RandomForestRegressor(random_state=42))
    ])

    # Define parameter grid
    param_grid = {
        'rf__n_estimators': [100, 200, 300, 400, 500, 600],
        'rf__min_samples_split': [5, 10, 20, 30],
        'rf__max_depth': [None, 10, 20, 30]
        ## for testing
        # 'rf__n_estimators': [100],
        # 'rf__min_samples_split': [5],
        # 'rf__max_depth': [10]
    }

    # Perform GridSearchCV
    from sklearn.model_selection import GridSearchCV

    grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1)
    grid_search.fit(X_train, y_train)

    # Print best parameters
    print("Best parameters:", grid_search.best_params_)


    # Calculate RMSE on training data
    y_train_pred = grid_search.best_estimator_.predict(X_train)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    print(f"RMSE on training data: {train_rmse:.4f}")

    # Calculate RMSE on test data
    y_test_pred = grid_search.best_estimator_.predict(X_test)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    print(f"RMSE on test data: {test_rmse:.4f}")
    
    # Save the best model
    best_params = grid_search.best_params_
    model_name_with_params = f"{model_name}_n{best_params['rf__n_estimators']}_s{best_params['rf__min_samples_split']}_d{best_params['rf__max_depth']}"
    joblib.dump(grid_search.best_estimator_, f'{model_name_with_params}.joblib')
    print(f"Best model saved as '{model_name_with_params}.joblib'")
    end_time = time.time()
    print(f"Grid search execution time: {end_time - start_time:.2f} seconds")
    return model_name_with_params

def make_predictions(X, label_encoder_movie, model_name):
    # Load the best model
    best_model = joblib.load(f'{model_name}.joblib')
    # record initial time
    start_time = time.time()
    # Initialize Ray
    ray.init()
    print(f"Ray is using {ray.cluster_resources()['CPU']} CPUs")

    @ray.remote
    def process_user(user_id, X, pipeline, label_encoder_movie):
        user_data = X[X['user_id'] == user_id]
        
        if len(user_data) > 0:
            all_movies = pd.DataFrame({'movie_name_encoded': X['movie_name_encoded'].unique()})
            all_movies['user_id'] = user_id
            
            for col in X.columns:
                if col not in all_movies.columns and col != 'movie_name_encoded':
                    all_movies[col] = user_data[col].iloc[0]
            
            all_movies = all_movies[pipeline.feature_names_in_]
            predicted_ratings = pipeline.predict(all_movies)
            all_movies['rating'] = predicted_ratings
            
            top_20_recommendations = all_movies.sort_values('rating', ascending=False).head(20)
            top_20_recommendations['movie_name'] = label_encoder_movie.inverse_transform(top_20_recommendations['movie_name_encoded'])
            
            return user_id, top_20_recommendations['movie_name'].tolist()
        
        return user_id, []

    # Process all users
    total_users = len(X['user_id'].unique())
    tasks = [process_user.remote(user_id, X, best_model, label_encoder_movie) for user_id in X['user_id'].unique()]

    start_time = time.time()
    user_recommendations = {}

    for i, (user_id, recommendations) in enumerate(tqdm(ray.get(tasks), desc="Processing users", total=total_users)):
        user_recommendations[user_id] = recommendations
        
        if (i + 1) % 10 == 0:
            elapsed_time = time.time() - start_time
            avg_time_per_user = elapsed_time / (i + 1)
            estimated_total_time = avg_time_per_user * total_users
            estimated_remaining_time = estimated_total_time - elapsed_time
            
            print(f"\nProcessed {i + 1}/{total_users} users")
            print(f"Estimated total time: {estimated_total_time:.2f} seconds")
            print(f"Estimated time remaining: {estimated_remaining_time:.2f} seconds")

    total_execution_time = time.time() - start_time
    print(f"\nTotal execution time: {total_execution_time:.2f} seconds")
    print(f"Average time per user: {total_execution_time / total_users:.2f} seconds")

    ray.shutdown()

    ## record end time
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    # Write recommendations to database
    mysql_conn = connect_db()
    cursor = mysql_conn.cursor()

    # Create the recommendations table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_recommendations (
        user_id INT PRIMARY KEY,
        top_20_recommendations TEXT
    )
    """
    cursor.execute(create_table_query)

    # Insert or update recommendations for each user
    insert_query = """
    INSERT INTO user_recommendations (user_id, top_20_recommendations)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE top_20_recommendations = VALUES(top_20_recommendations)
    """
    
    for user_id, recommendations in tqdm(user_recommendations.items(), desc="Saving recommendations"):
        recommendations_str = ','.join(map(lambda x: str(x).replace(' ', '+'), recommendations))
        cursor.execute(insert_query, (user_id, recommendations_str))

    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()
    
    print(f"User recommendations have been saved to the 'user_recommendations' table in the database")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Movie Recommendation System")
    parser.add_argument('--mode', choices=['train', 'predict'], help="Mode of operation: 'train' for grid search and model saving, 'predict' for making predictions")
    parser.add_argument('--model_name', default='best_model', help="Base name of the model file (without .joblib extension)")
    args = parser.parse_args()

    X_train, X_test, y_train, y_test, label_encoder_movie = load_and_preprocess_data()

    if args.mode == 'train':
        model_name_with_params = grid_search_and_save_model(X_train, y_train, args.model_name)
        print(f"Use this model name for prediction: {model_name_with_params}")
    elif args.mode == 'predict':
        make_predictions(X_train, label_encoder_movie, args.model_name)