import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
import os
import joblib
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Import the module to test
from main.recommendation_model.rf_model_pipeline import (
    connect_db,
    load_and_preprocess_data,
    grid_search_and_save_model,
    make_predictions
)

@pytest.fixture
def mock_env_vars():
    with patch.dict(os.environ, {
        'DB_PASSWORD': 'fake_password',
        'DB_NAME': 'fake_db'
    }):
        yield

@pytest.fixture
def sample_data():
    # Create fake movie data
    data = pd.DataFrame({
        'user_id': [1, 1, 2, 2],
        'movie_name': ['Movie A', 'Movie B', 'Movie A', 'Movie C'],
        'user_rating': [4.5, 3.0, 5.0, 2.5],
        'tmdb_rating': [7.5, 6.8, 7.5, 6.2],
        'genre': ['Action', 'Comedy', 'Action', 'Drama'],
        'release_year': [2020, 2019, 2020, 2021]
    })
    return data

@pytest.fixture
def mock_db_connection(sample_data):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock pd.read_sql to return our sample data
    with patch('pandas.read_sql', return_value=sample_data):
        yield mock_conn

def test_connect_db(mock_env_vars):
    with patch('mysql.connector.connect') as mock_connect:
        connect_db()
        mock_connect.assert_called_once_with(
            host="localhost",
            user="mluser",
            password="fake_password",
            database="fake_db"
        )

def test_load_and_preprocess_data(mock_db_connection, sample_data):
    with patch('main.recommendation_model.rf_model_pipeline.connect_db', return_value=mock_db_connection):
        X_train, X_test, y_train, y_test, label_encoder = load_and_preprocess_data()
        
        # Check if data was split correctly
        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(y_train, pd.Series)
        assert len(X_train) + len(X_test) == len(sample_data)
        assert 'movie_name_encoded' in X_train.columns
        assert 'movie_name' not in X_train.columns
        assert 'tmdb_rating' not in X_train.columns

def test_grid_search_and_save_model():
    # Create small fake training data
    X_train = pd.DataFrame({
        'movie_name_encoded': [0, 1, 2],
        'user_id': [1, 1, 2],
        'genre': [0, 1, 2],  # Changed to encoded values instead of strings
        'release_year': [2020, 2019, 2021]
    })
    y_train = pd.Series([4.5, 3.0, 5.0])
    
    # Use smaller parameter grid for testing
    with patch('sklearn.model_selection.GridSearchCV') as mock_grid:
        mock_grid.return_value.best_params_ = {
            'rf__n_estimators': 100,
            'rf__min_samples_split': 5,
            'rf__max_depth': 10
        }
        
        # Create and fit a pipeline to use as best_estimator_
        pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy='mean')),  # Changed to 'mean' strategy for numerical data
            ('rf', RandomForestRegressor())
        ])
        pipeline.fit(X_train, y_train)
        
        mock_grid.return_value.best_estimator_ = pipeline
        
        model_name = grid_search_and_save_model(X_train, X_train, y_train, y_train, "test_model")
        assert model_name.startswith("test_model")
        assert "n100_s5_d10" in model_name

@pytest.mark.skip(reason="Ray initialization makes this test complex to run in CI/CD")
def test_make_predictions(sample_data):
    # This is a basic structure for testing make_predictions
    # You might want to skip this in CI/CD due to Ray initialization
    X = sample_data.copy()
    label_encoder = Mock()
    label_encoder.inverse_transform.return_value = ['Movie A', 'Movie B']
    
    # Create and save a dummy model
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('rf', RandomForestRegressor(n_estimators=10))
    ])
    model_name = "test_model"
    joblib.dump(pipeline, f'{model_name}.joblib')
    
    with patch('ray.init'), patch('ray.shutdown'):
        make_predictions(X, label_encoder, model_name)
        
    # Cleanup
    if os.path.exists(f'{model_name}.joblib'):
        os.remove(f'{model_name}.joblib')
