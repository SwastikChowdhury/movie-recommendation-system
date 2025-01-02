import mlflow
import mlflow.sklearn
import ray
from tqdm import tqdm
import time
import pandas as pd
from datetime import datetime

def load_production_model(model_name):
    """Load the production model from MLflow model registry"""
    model = mlflow.sklearn.load_model(f"models:/{model_name}/Production")
    return model

def make_predictions(X, label_encoder_movie, model_name, data_version_info):
    start_time = time.time()
    client = mlflow.tracking.MlflowClient()


    versions = client.search_model_versions(f"name='{model_name}'")
    latest_version = max(int(v.version) for v in versions)
    print(f"Latest version of {model_name} is {latest_version}")

    # Transition it to Production
    client.transition_model_version_stage(
        name=model_name,
        version=latest_version,
        stage="Production"
    )


    print(f"Model {model_name} version {latest_version} transitioned to Production stage")
    
    with mlflow.start_run(run_name=f"prediction_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log data version information
        mlflow.log_params({
            f"data_{key}": str(value) 
            for key, value in data_version_info.items()
        })
        # Log model info to MLflow
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("model_version", latest_version)
        mlflow.log_param("model_stage", "Production")

        # Load the model from MLflow
        model = load_production_model(model_name)
        
        # Initialize Ray
        ray.init()
        
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
        
        # Process users and log metrics
        total_users = len(X['user_id'].unique())
        tasks = [process_user.remote(user_id, X, model, label_encoder_movie) 
                for user_id in X['user_id'].unique()]
        
        user_recommendations = {}
        for i, (user_id, recommendations) in enumerate(tqdm(ray.get(tasks), 
                                                          desc="Processing users", 
                                                          total=total_users)):
            user_recommendations[user_id] = recommendations
        
        # Log prediction metrics
        execution_time = time.time() - start_time
        mlflow.log_metric("prediction_time_seconds", execution_time)
        mlflow.log_metric("users_processed", total_users)
        mlflow.log_metric("avg_time_per_user", execution_time/total_users)
        
        ray.shutdown()
        return user_recommendations 