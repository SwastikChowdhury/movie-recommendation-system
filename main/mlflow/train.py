import mlflow
import mlflow.sklearn
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import time
from datetime import datetime

def log_model_metrics(y_true, y_pred, prefix=""):
    """Log various model metrics to MLflow"""
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    
    mlflow.log_metric(f"{prefix}rmse", rmse)
    mlflow.log_metric(f"{prefix}mae", mae)
    mlflow.log_metric(f"{prefix}r2", r2)
    
    return rmse, mae, r2

def train_model(pipeline, param_grid, X_train, X_test, y_train, y_test, model_name, data_version_info):
    start_time = time.time()
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"rf_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log data version information
        mlflow.log_params({
            f"data_{key}": str(value) 
            for key, value in data_version_info.items()
        })
        
        # Log existing parameters
        mlflow.log_params({
            "model_type": "RandomForestRegressor",
            "cv_folds": 5,
            "test_size": 0.2,
            "random_state": 42
        })
        print("starting grid search")
        # Perform GridSearchCV
        grid_search = GridSearchCV(pipeline, param_grid, cv=5, 
                                 scoring='neg_mean_squared_error', n_jobs=-1)
        grid_search.fit(X_train, y_train)
        print("grid search completed")
        # Log best parameters
        mlflow.log_params(grid_search.best_params_)
        print("best params logged")
        # Calculate and log metrics
        y_train_pred = grid_search.best_estimator_.predict(X_train)
        y_test_pred = grid_search.best_estimator_.predict(X_test)
        print("predictions made")
        train_metrics = log_model_metrics(y_train, y_train_pred, "train_")
        test_metrics = log_model_metrics(y_test, y_test_pred, "test_")
        print("metrics logged")
        # Log execution time
        execution_time = time.time() - start_time
        mlflow.log_metric("execution_time_seconds", execution_time)
        
        ##
        # Register the model in MLflow
        mlflow.sklearn.log_model(
            grid_search.best_estimator_,
            "model",
            registered_model_name=model_name
        )
        # Log feature importance
        feature_importance = grid_search.best_estimator_.named_steps['rf'].feature_importances_
        for idx, importance in enumerate(feature_importance):
            mlflow.log_metric(f"feature_importance_{idx}", importance)
        
        return grid_search.best_estimator_, grid_search.best_params_ 