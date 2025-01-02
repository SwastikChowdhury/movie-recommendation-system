import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import aiohttp
from aioresponses import aioresponses
import mysql.connector
import sys
import os
from dotenv import load_dotenv

# Setup project path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Load environment variables
load_dotenv()

api_key = os.environ.get('TMDB_API_KEY')
if not api_key:
    raise ValueError("TMDB_API_KEY environment variable is not set")

from main.data_fetch.tmdb import (
    fetch_movies,
    get_movies_by_date_range,
    connect_db,
    get_all_movies
)

# Test Constants
SAMPLE_MOVIE_RESPONSE = {
    'results': [
        {
            'title': 'Test Movie',
            'release_date': '2024-01-01',
            'vote_average': 7.5,
            'overview': 'Test movie overview'
        }
    ],
    'total_pages': 1
}

# Fixtures
@pytest.fixture
def mock_env_vars():
    """Fixture to mock environment variables."""
    with patch.dict('os.environ', {
        'TMDB_API_KEY': api_key,
        'DB_PASSWORD': 'fake_password',
        'DB_NAME': 'fake_db'
    }):
        yield

@pytest.fixture
def mock_db_connection():
    """Fixture to mock database connection and cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    with patch('mysql.connector.connect', return_value=mock_conn):
        yield mock_conn, mock_cursor

@pytest.mark.asyncio
async def test_fetch_movies_success():
    """Test successful movie fetch from API."""
    with aioresponses() as mocked:
        url = 'https://api.themoviedb.org/3/discover/movie'
        # Include all required query parameters that the API expects
        params = {
            'api_key': api_key,
            'include_adult': 'false',
            'include_video': 'false',
            'language': 'en-US',
            'page': '1',
            'sort_by': 'popularity.desc'
        }
        
        # Create the full URL with query parameters
        query_url = f"{url}?api_key={api_key}"
        for key, value in params.items():
            if key != 'api_key':  # Skip api_key as it's already added
                query_url += f"&{key}={value}"
        
        mocked.get(query_url, payload=SAMPLE_MOVIE_RESPONSE)
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_movies(session, url, params)
        
        assert result == SAMPLE_MOVIE_RESPONSE

@pytest.mark.asyncio
async def test_fetch_movies_error():
    """Test error handling during movie fetch."""
    with aioresponses() as mocked:
        url = 'https://api.themoviedb.org/3/discover/movie'
        params = {'api_key': api_key}
        
        # Mock the exact URL pattern including the api_key
        mocked.get(f"{url}?api_key={api_key}", status=404)
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_movies(session, url, params)
        
        assert result is None

# Database Tests
def test_connect_db_success(mock_env_vars, mock_db_connection):
    """Test successful database connection."""
    mock_conn, _ = mock_db_connection
    result = connect_db()
    assert result == mock_conn

def test_connect_db_failure(mock_env_vars):
    """Test database connection failure."""
    with patch('mysql.connector.connect', side_effect=mysql.connector.Error):
        with pytest.raises(mysql.connector.Error):
            connect_db()

# Integration Tests
@pytest.mark.asyncio
async def test_get_movies_by_date_range(mock_db_connection):
    """Test fetching movies for a specific date range."""
    mock_conn, mock_cursor = mock_db_connection
    
    with aioresponses() as mocked:
        url = 'https://api.themoviedb.org/3/discover/movie'
        
        # Mock the exact URL pattern with all required parameters
        params = {
            'api_key': api_key,
            'include_adult': 'false',
            'include_video': 'false',
            'language': 'en-US',
            'page': '1',
            'sort_by': 'popularity.desc',
            'primary_release_date.gte': '2024-01-01',
            'primary_release_date.lte': '2024-12-31'
        }
        
        # Construct the full URL with query parameters
        query_url = f"{url}?api_key={api_key}"
        for key, value in params.items():
            if key != 'api_key':  # Skip api_key as it's already added
                query_url += f"&{key}={value}"
        
        mocked.get(query_url, payload=SAMPLE_MOVIE_RESPONSE)
        
        async with aiohttp.ClientSession() as session:
            result = await get_movies_by_date_range(
                session,
                '2024-01-01',
                '2024-12-31',
                api_key,
                mock_cursor
            )
        
        assert result == 1
        # Verify that the database insert was called
        mock_cursor.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_movies_by_date_range_db_error(mock_db_connection):
    """Test handling of database errors during movie fetch."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.execute.side_effect = mysql.connector.Error("Database error")
    
    with aioresponses() as mocked:
        url = 'https://api.themoviedb.org/3/discover/movie'
        
        # Mock the exact URL pattern with all required parameters
        params = {
            'api_key': api_key,
            'include_adult': 'false',
            'include_video': 'false',
            'language': 'en-US',
            'page': '1',
            'sort_by': 'popularity.desc',
            'primary_release_date.gte': '2024-01-01',
            'primary_release_date.lte': '2024-12-31'
        }
        
        # Construct the full URL with query parameters
        query_url = f"{url}?api_key={api_key}"
        for key, value in params.items():
            if key != 'api_key':  # Skip api_key as it's already added
                query_url += f"&{key}={value}"
        
        mocked.get(query_url, payload=SAMPLE_MOVIE_RESPONSE)
        
        async with aiohttp.ClientSession() as session:
            result = await get_movies_by_date_range(
                session,
                '2024-01-01',
                '2024-12-31',
                api_key,
                mock_cursor
            )
        
        # Verify the behavior when database insert fails
        assert result == 1  # Still counts the movie even if DB insert fails
        mock_cursor.execute.assert_called_once()  # Verify execute was called once
        assert isinstance(mock_cursor.execute.side_effect, mysql.connector.Error)  # Verify it was a database error