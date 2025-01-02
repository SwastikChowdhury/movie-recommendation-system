import pytest
import pandas as pd
import mysql.connector
from mysql.connector import Error
from unittest.mock import patch, MagicMock
import logging
import numpy as np
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Import the functions to be tested
from main.data_fetch.pre_processing import (
    connect_db, load_data_from_mysql, format_movie_name, clean_text,
    clean_tmdb_data, process_movies_data, process_kafka_data,
    create_table, write_to_mysql, extract_year
)

# Test data fixtures
@pytest.fixture
def mock_movies_data():
    return pd.DataFrame({
        'movieId': [1, 2],
        'title': ['Toy Story (1995)', 'Jumanji (1995)'],
        'genres': ['Adventure|Animation|Children', 'Adventure|Children|Fantasy']
    })

@pytest.fixture
def mock_kafka_data():
    return pd.DataFrame({
        'log_type': ['rating', 'watch'],
        'userid': [1, 1],
        'movieid': ['Toy_Story+1995', 'Jumanji+1995'],
        'rating': [4.5, None],
        'minute': [None, '30.5']
    })

@pytest.fixture
def mock_tmdb_data():
    return pd.DataFrame({
        'title': ['Toy Story', 'Jumanji'],
        'release_date': ['1995-11-22', '1995-12-15'],
        'vote_average': [8.0, 7.0]
    })

# Test database connection
@patch('mysql.connector.connect')
def test_connect_db(mock_connect):
    mock_connect.return_value = MagicMock()
    conn = connect_db()
    assert conn is not None
    mock_connect.assert_called_once()

# Test data loading
def test_load_data_from_mysql():
    mock_conn = MagicMock()
    mock_df = pd.DataFrame({'col1': [1, 2]})
    with patch('pandas.read_sql', return_value=mock_df):
        df = load_data_from_mysql(mock_conn, 'test_table')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

@pytest.mark.parametrize("input_name,expected", [
    ('Toy_Story+1995', 'ToyStory (1995)'),
    ('Jumanji+1995', 'Jumanji (1995)')
])
def test_format_movie_name(input_name, expected):
    result = format_movie_name(input_name)
    assert result == expected

# Test text cleaning
@pytest.mark.parametrize("input_text,expected", [
    ('Toy Story!', 'toy story'),
    ('The Movie: Part 2', 'the movie part 2')
])
def test_clean_text(input_text, expected):
    assert clean_text(input_text) == expected

# Test TMDB data cleaning
def test_clean_tmdb_data(mock_tmdb_data):
    cleaned_df = clean_tmdb_data(mock_tmdb_data)
    assert 'movie_name' in cleaned_df.columns
    assert 'tmdb_rating' in cleaned_df.columns
    assert cleaned_df['tmdb_rating'].max() <= 5.0

# Test movies data processing
def test_process_movies_data(mock_movies_data):
    processed_df = process_movies_data(mock_movies_data)
    assert 'movie_name' in processed_df.columns
    assert 'Adventure' in processed_df.columns
    assert 'Animation' in processed_df.columns

# Test kafka data processing
def test_process_kafka_data(mock_kafka_data):
    ratings_df, watch_df = process_kafka_data(mock_kafka_data)
    assert 'user_rating' in ratings_df.columns
    assert 'minute' in watch_df.columns
    assert isinstance(watch_df['minute'].iloc[0], float)

# Test table creation
def test_create_table():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    create_table(mock_conn)
    
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()

# Test MySQL writing
def test_write_to_mysql():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    test_df = pd.DataFrame({
        'movie_name': ['test_movie'],
        'user_id': [1],
        'rating': [4.5]
    })
    
    write_to_mysql(mock_conn, test_df, 'test_table')
    
    mock_cursor.executemany.assert_called_once()
    mock_conn.commit.assert_called_once()

# Test year extraction
@pytest.mark.parametrize("movie_name,expected_year", [
    ('Toy Story 1995', 1995),
    ('Movie without year', 0),
    ('Invalid year (abcd)', 0)
])
def test_extract_year(movie_name, expected_year):
    assert extract_year(movie_name) == expected_year

# Test error handling
def test_load_data_from_mysql_error():
    mock_conn = MagicMock()
    with patch('pandas.read_sql', side_effect=Exception("Database error")):
        with pytest.raises(Exception):
            load_data_from_mysql(mock_conn, 'test_table')