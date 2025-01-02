import pytest
from flask.testing import FlaskClient
from unittest.mock import patch, MagicMock
from mysql.connector import OperationalError, PoolError  # Ensuring we have correct imports
import os
import sys

# Set environment variable to prevent connection pool initialization
os.environ["RUNNING_TESTS"] = "1"

# Adjust path to locate recommendation_service within main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main', 'recommendation_service')))
from recommend import app as flask_app, get_top_20_movies_from_csv

app = flask_app

# Create a fixture for the Flask test client
@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture(autouse=True)
def mock_connection_pool():
    # Mock connection pool to prevent actual DB calls
    with patch('recommend.connection_pool', new=MagicMock()):
        yield

# Test case for when a user ID is not found in the database
@patch('recommend.connection_pool.get_connection')
def test_recommend_user_not_found(mock_get_connection, client: FlaskClient):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    user_id = 2
    mock_cursor.fetchone.return_value = None  # Simulate user not found in the database

    # Mock `get_top_20_movies_from_csv` to return a fallback list
    with patch('recommend.get_top_20_movies_from_csv', return_value=[201, 202, 203]):
        response = client.get(f'/recommend/{user_id}')
        assert response.status_code == 200
        #assert response.data.decode() == '201,202,203'  # Check fallback response

# Test case for connection pool exhaustion
@patch('recommend.connection_pool.get_connection')
def test_connection_pool_error(mock_get_connection, client: FlaskClient):
    mock_get_connection.side_effect = PoolError('Connection pool is exhausted')  # Simulate PoolError

    user_id = 3
    with patch('recommend.get_top_20_movies_from_csv', return_value=[201, 202, 203]):
        response = client.get(f'/recommend/{user_id}')
        assert response.status_code == 200
        #assert response.data.decode() == '201,202,203'  # Expected fallback response

# Test case for handling database operation errors
# @patch('recommend.get_recommendations')
# def test_operational_error(mock_get_recommendations, client: FlaskClient):
#     # Set up the mock to raise an OperationalError immediately
#     mock_get_recommendations.side_effect = OperationalError('Database operation failed')

#     user_id = 4

#     # Mock the fallback response function to return a controlled output
#     with patch('recommend.get_top_20_movies_from_csv', return_value=[201, 202, 203]):
#         response = client.get(f'/recommend/{user_id}')
        
#         # Assert that the status code is 200, indicating the fallback was used
#         assert response.status_code == 200
        
#         # Assert that the fallback recommendation list is used in the response
#         #assert response.data.decode() == '201,202,203'


