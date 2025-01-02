import mlflow
import argparse
from train import train_model
from predict import make_predictions
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from utils import load_and_preprocess_data, save_recommendations

def main():
    parser = argparse.ArgumentParser(description="Movie Recommendation System with MLflow")
    parser.add_argument('--mode', choices=['train', 'predict'], required=True,
                      help="Mode of operation: 'train' or 'predict'")
    parser.add_argument('--model_name', default='movie_recommender',
                      help="Name of the model in MLflow registry")
    args = parser.parse_args()
    
    # Set MLflow tracking URI
    mlflow.set_tracking_uri("http://127.0.0.1:6001")
    mlflow.set_experiment("movie_recommendation_system")
    
    if args.mode == 'train':
        # Load and preprocess data
        X_train, X_test, y_train, y_test, label_encoder_movie, data_version_info = load_and_preprocess_data()
        
        # Create pipeline and parameter grid
        pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('rf', RandomForestRegressor(random_state=42))
        ])
        
        param_grid = {
            'rf__n_estimators': [100, 300],
            'rf__min_samples_split': [10, 20, 30],
            'rf__max_depth': [10, 20, 30]
        }
        
        # Train model with MLflow tracking
        train_model(pipeline, param_grid, X_train, X_test, y_train, y_test, 
                                            args.model_name, data_version_info)
        

        
    elif args.mode == 'predict':
        # Load data and make predictions
        X_train, _, _, _, label_encoder_movie, data_version_info = load_and_preprocess_data()
        recommendations = make_predictions(X_train, label_encoder_movie, args.model_name, data_version_info)
        save_recommendations(recommendations, args.model_name)

if __name__ == "__main__":
    main() 