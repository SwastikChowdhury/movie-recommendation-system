import pytest
from unittest.mock import patch, MagicMock
from mysql.connector import OperationalError, PoolError
import sys
import os

# Add the path to the directory containing online_evaluation.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main', 'eval')))

from online_evaluation import (
    connect_to_database,
    fetch_user_recommendations,
    normalize_title,
    evaluate_recommendation_usage,
    store_evaluation_results
)

# Test connect_to_database
@patch('online_evaluation.pooling.MySQLConnectionPool')
def test_connect_to_database_success(mock_pool):
    mock_pool.return_value = MagicMock()
    connection_pool = connect_to_database()
    assert connection_pool is not None

@patch('online_evaluation.pooling.MySQLConnectionPool')
def test_connect_to_database_failure(mock_pool):
    mock_pool.side_effect = OperationalError("Connection error")
    connection_pool = connect_to_database()
    assert connection_pool is None

# Test fetch_user_recommendations
@patch('online_evaluation.connect_to_database')
def test_fetch_user_recommendations(mock_connect_to_database):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_to_database.return_value.get_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [{'user_id': 1, 'recommendations': "['Toy Story', 'Inception']"}]

    recommendations = fetch_user_recommendations(mock_connect_to_database.return_value)
    assert recommendations == {'1': {'toy+story', 'inception'}}

# Test normalize_title
def test_normalize_title():
    title = "Toy Story 1995"
    normalized = normalize_title(title)
    assert normalized == "toy+story+1995"

# Test evaluate_recommendation_usage
@patch('online_evaluation.connect_to_database')
@patch('online_evaluation.fetch_user_recommendations')
def test_evaluate_recommendation_usage(mock_fetch_user_recommendations, mock_connect_to_database):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_to_database.return_value.get_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        {'userid': 1, 'movieid': 'Toy Story'},
        {'userid': 2, 'movieid': 'Inception'}
    ]
    mock_fetch_user_recommendations.return_value = {'1': {'toy+story'}, '2': {'inception'}}

    results, match_rate = evaluate_recommendation_usage(mock_connect_to_database.return_value, mock_fetch_user_recommendations.return_value)
    assert len(results) == 2  # Ensure we have results for both users

# Test store_evaluation_results
@patch('online_evaluation.connect_to_database')
def test_store_evaluation_results(mock_connect_to_database):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_to_database.return_value.get_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    results = [(1, 'Toy Story', True), (2, 'Inception', False)]
    store_evaluation_results(mock_connect_to_database.return_value, results)
    mock_conn.commit.assert_called_once()

