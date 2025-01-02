import pytest
import pandas as pd
import mysql.connector
from mysql.connector import Error
from unittest.mock import patch, MagicMock
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Import the functions to be tested
from main.data_fetch.movies_to_db import connect_db, insert_data_from_csv

# Fixture for mocking environment variables
@pytest.fixture
def mock_env_vars(monkeypatch):
    """
    Fixture to mock environment variables
    """
    monkeypatch.setenv('DB_PASSWORD', 'test_password')
    monkeypatch.setenv('DB_NAME', 'test_db')

# Fixture for mocking CSV data
@pytest.fixture
def mock_csv_data():
    """
    Fixture to create mock CSV data that matches the actual movie dataset format
    """
    return pd.DataFrame({
        'movieId': [1, 2, 3, 4],
        'title': ['Toy Story (1995)', 
                 'Jumanji (1995)', 
                 'Grumpier Old Men (1995)', 
                 'Waiting to Exhale (1995)'],
        'genres': ['Adventure|Animation|Children|Comedy|Fantasy',
                  'Adventure|Children|Fantasy',
                  'Comedy|Romance',
                  'Comedy|Drama|Romance']
    })

@patch('mysql.connector.connect')
def test_connect_db(mock_connect, mock_env_vars):
    """
    Test successful database connection
    """
    mock_connect.return_value = MagicMock()
    conn = connect_db()
    
    assert conn is not None
    mock_connect.assert_called_once_with(
        host="localhost",
        user="mluser",
        password="test_password",
        database="test_db"
    )

@patch('mysql.connector.connect')
def test_connect_db_error(mock_connect, mock_env_vars):
    """
    Test database connection error handling
    """
    mock_connect.side_effect = Error("Connection error")
    with pytest.raises(SystemExit):
        connect_db()

@patch('pandas.read_csv')
@patch('main.data_fetch.movies_to_db.connect_db')
def test_insert_data_from_csv(mock_connect_db, mock_read_csv, mock_csv_data):
    """
    Test successful data insertion from CSV
    """
    # Mock database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_db.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Mock CSV file reading
    mock_read_csv.return_value = mock_csv_data

    # Execute function
    insert_data_from_csv('dummy.csv')

    # Print actual calls for debugging
    print("\nActual calls:")
    for call in mock_cursor.execute.call_args_list:
        print(call)

    # Verify SQL execution
    expected_sql = """
            INSERT INTO movies (movieId, title, genres)
            VALUES (%s, %s, %s)
            """
    assert mock_cursor.execute.call_count == 4  # Now checking for 4 movies
    
    mock_cursor.execute.assert_any_call(expected_sql, 
        (1, 'Toy Story (1995)', 'Adventure|Animation|Children|Comedy|Fantasy'))
    mock_cursor.execute.assert_any_call(expected_sql, 
        (2, 'Jumanji (1995)', 'Adventure|Children|Fantasy'))
    mock_cursor.execute.assert_any_call(expected_sql, 
        (3, 'Grumpier Old Men (1995)', 'Comedy|Romance'))
    mock_cursor.execute.assert_any_call(expected_sql, 
        (4, 'Waiting to Exhale (1995)', 'Comedy|Drama|Romance'))

    # Verify transaction handling
    mock_conn.commit.assert_called_once()

    # Verify connection cleanup
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

@patch('pandas.read_csv')
@patch('main.data_fetch.movies_to_db.connect_db')
def test_insert_data_from_csv_db_error(mock_connect_db, mock_read_csv, mock_csv_data):
    """
    Test database error handling during insertion
    """
    # Mock setup
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_db.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_read_csv.return_value = mock_csv_data

    # Simulate database error
    mock_cursor.execute.side_effect = Error("Database error")

    # Execute function
    insert_data_from_csv('dummy.csv')

    # Verify error handling
    mock_conn.rollback.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

@patch('pandas.read_csv')
@patch('main.data_fetch.movies_to_db.connect_db')
def test_insert_data_from_csv_file_error(mock_connect_db, mock_read_csv):
    """
    Test file reading error handling
    """
    # Mock file reading error
    mock_read_csv.side_effect = Exception("File reading error")
    mock_conn = mock_connect_db.return_value

    # Execute function
    insert_data_from_csv('dummy.csv')

    # Verify connection cleanup
    mock_conn.cursor.return_value.close.assert_called_once()
    mock_conn.close.assert_called_once()