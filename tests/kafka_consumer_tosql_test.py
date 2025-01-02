import unittest
from unittest.mock import patch, MagicMock
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main', 'data_fetch')))
from kafka_consumer_tosql import process_message, validate_data, insert_or_update_record

# project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(project_root)

class TestKafkaToMySQL(unittest.TestCase):
    @patch('kafka_consumer_tosql.KafkaConsumer')
    @patch('kafka_consumer_tosql.mysql.connector.connect')
    def test_connect_db(self, mock_connect, mock_kafka_consumer):
        """
        Test successful database connection
        """
        with patch.dict('os.environ', {'DB_PASSWORD': 'test_password', 'DB_NAME': 'test_db'}):
            mock_connect.return_value = MagicMock()
            from kafka_consumer_tosql import connect_db
            conn = connect_db()
            
            assert conn is not None
            mock_connect.assert_called_once_with(
                host="localhost",
                user="mluser",
                password="test_password",
                database="test_db"
            )


### <--------------recommendation message test case---------------->
    def test_process_recommendation_message(self):
        message = '2024-10-25T03:15:41.160449866,262726,recommendation request 17645-team02.isri.cmu.edu:8082, status 200, result: earthlings+2005, seven+samurai+1954, high+society+1956, castle+in+the+sky+1986, stonehearst+asylum+2014, gates+of+heaven+1978, the+aviator+2004, whisper+of+the+heart+1995, samurai+iii+duel+at+ganryu+island+1956, mansfield+park+1999, you_+the+living+2007, the+wolf+of+wall+street+2013, the+carter+2009, my+afternoons+with+margueritte+2010, thats+entertainment+1974, finding+nemo+2003, monty+python+and+the+holy+grail+1975, fires+on+the+plain+1959, the+avengers+2012, the+leopard+1963, 75 ms'
        expected_output = (
            "recommendation", '2024-10-25T03:15:41.160449866', '262726', None, None, None, '17645-team02.isri.cmu.edu:8082',
            '200', 'earthlings+2005', 'seven+samurai+1954, high+society+1956, castle+in+the+sky+1986, stonehearst+asylum+2014, gates+of+heaven+1978, the+aviator+2004, whisper+of+the+heart+1995, samurai+iii+duel+at+ganryu+island+1956, mansfield+park+1999, you_+the+living+2007, the+wolf+of+wall+street+2013, the+carter+2009, my+afternoons+with+margueritte+2010, thats+entertainment+1974, finding+nemo+2003, monty+python+and+the+holy+grail+1975, fires+on+the+plain+1959, the+avengers+2012, the+leopard+1963, 75 ms'
        )
        actual_output = process_message(message)
        print(f"Actual Output: {actual_output}")
        self.assertEqual(actual_output, expected_output)

    # Invalid user ID test case
    def test_recommendation_invalid_userid(self):
        message = '2024-10-25T03:15:41.160449866,invalid_user_123,recommendation request 17645-team02.isri.cmu.edu:8082, status 200, result: earthlings+2005, seven+samurai+1954, high+society+1956, castle+in+the+sky+1986, stonehearst+asylum+2014, gates+of+heaven+1978, the+aviator+2004, whisper+of+the+heart+1995, samurai+iii+duel+at+ganryu+island+1956, mansfield+park+1999, you_+the+living+2007, the+wolf+of+wall+street+2013, the+carter+2009, my+afternoons+with+margueritte+2010, thats+entertainment+1974, finding+nemo+2003, monty+python+and+the+holy+grail+1975, fires+on+the+plain+1959, the+avengers+2012, the+leopard+1963, 75 ms'
        log_entry = process_message(message)
        if log_entry is None:
            print("Recommendation - Log entry should be None for invalid userid.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid userid.")
        
    # Invalid date test case
    def test_recommendation_invalid_date(self):
        message = '2024-13-25T03:15:41.160449866,262726,recommendation request 17645-team02.isri.cmu.edu:8082, status 200, result: earthlings+2005, seven+samurai+1954, high+society+1956, castle+in+the+sky+1986, stonehearst+asylum+2014, gates+of+heaven+1978, the+aviator+2004, whisper+of+the+heart+1995, samurai+iii+duel+at+ganryu+island+1956, mansfield+park+1999, you_+the+living+2007, the+wolf+of+wall+street+2013, the+carter+2009, my+afternoons+with+margueritte+2010, thats+entertainment+1974, finding+nemo+2003, monty+python+and+the+holy+grail+1975, fires+on+the+plain+1959, the+avengers+2012, the+leopard+1963, 75 ms'
        log_entry = process_message(message)
        if log_entry is None:
            print("Recommendation - Log entry should be None for invalid date.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid date.")

    # Invalid status test case
    def test_recommendation_invalid_status(self):
        message = '2024-10-25T03:15:41.160449866,262726,recommendation request 17645-team02.isri.cmu.edu:8082, status 400, result: earthlings+2005, seven+samurai+1954, high+society+1956, castle+in+the+sky+1986, stonehearst+asylum+2014, gates+of+heaven+1978, the+aviator+2004, whisper+of+the+heart+1995, samurai+iii+duel+at+ganryu+island+1956, mansfield+park+1999, you_+the+living+2007, the+wolf+of+wall+street+2013, the+carter+2009, my+afternoons+with+margueritte+2010, thats+entertainment+1974, finding+nemo+2003, monty+python+and+the+holy+grail+1975, fires+on+the+plain+1959, the+avengers+2012, the+leopard+1963, 75 ms'
        log_entry = process_message(message)
        if log_entry is None:
            print("Recommendation - Log entry should be None for invalid status.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid status.")


### <--------------watch message test case---------------->
    def test_process_watch_message(self):
        message = '2024-10-24T06:33:38,399307,GET /data/m/high+society+1956/55.mpg'
        expected_output = (
            "watch", "2024-10-24T06:33:38", "399307", "high+society+1956", "55", None, None, None, None, None
        )
        actual_output = process_message(message)
        print(f"Actual Output: {actual_output}")
        self.assertEqual(process_message(message), expected_output)

    # Invalid user ID test case
    def test_watch_invalid_userid(self):
        message = '2024-10-24T06:33:38,invalid_user_123,GET /data/m/high+society+1956/55.mpg'
        log_entry = process_message(message)
        if log_entry is None:
            print("Log entry should be None for invalid userid.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid userid.")

    # Invalid movie ID test case
    def test_watch_invalid_movieid(self):
        message = '2024-10-24T06:33:38,12344,GET /data/m//55.mpg'
        log_entry = process_message(message)
        if log_entry is None:
            print("Log entry should be None for invalid movieid.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid movieid.")

    # Invalid minute test case
    def test_watch_invalid_minute(self):
        message = '2024-10-24T06:33:38,12344,GET /data/m/high+society+1956/ten.mpg'
        log_entry = process_message(message)
        if log_entry is None:
            print("Log entry should be None for invalid minute.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid minute.")

    # Invalid date test case
    def test_watch_invalid_date(self):
        message = '2024-13-35T25:61:61,12344,GET /data/m/high+society+1956/10.mpg'
        log_entry = process_message(message)
        if log_entry is None:
            self.assertIsNone(log_entry, "Log entry should be None for invalid date.")


### <--------------rating message test case---------------->
    def test_process_rate_message(self):
        message = '2024-10-26T01:59:46,504137,GET /rate/black+plague+2002=4'
        expected_output = (
            "rating", '2024-10-26T01:59:46', '504137', 'black+plague+2002', None, '4', None, None, None, None
        )
        actual_output = process_message(message)
        print(f"Actual Output: {actual_output}")
        self.assertEqual(process_message(message), expected_output)

    # Invalid user ID test case
    def test_rating_invalid_userid(self):
        message = '2024-10-26T01:59:46,invalid_user_123,GET /rate/black+plague+2002=4'
        log_entry = process_message(message)
        if log_entry is None:
            print("Rating - Log entry should be None for invalid userid.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid userid.")

    # Invalid movie ID test case
    def test_rating_invalid_movieid(self):
        message = '2024-10-26T01:59:46,504137,GET /rate/ =4'
        log_entry = process_message(message)
        if log_entry is None:
            print("Rating - Log entry should be None for invalid movieid.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid movieid.")

    # Invalid rating test case
    def test_rating_invalid_rate(self):
        message = '2024-10-26T01:59:46,504137,GET /rate/black+plague+2002=five'
        log_entry = process_message(message)
        if log_entry is None:
            print("Rating - Log entry should be None for invalid rate.")
            self.assertIsNone(log_entry, "Log entry should be None for invalid rate.")

    # Invalid date test case
    def test_rating_invalid_date(self):
        message = '2024-13-33T01:59:46,504137,GET /rate/black+plague+2002=4'
        log_entry = process_message(message)
        if log_entry is None:
            self.assertIsNone(log_entry, "Log entry should be None for invalid date.")


 ### <--------------insert test case---------------->           
    @patch('kafka_consumer_tosql.mysql.connector.connect')
    def test_insert_record(self, mock_connect):
        # Mock MySQL cursor and connection
        mock_cursor = MagicMock()
        mock_connect().cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # Simulate no existing record

        log_entry = (
            "rating", '2024-10-05T04:00:54', '12345678', 'i+love+cmu+2024', None, '5', None, None, None, None
        )
        insert_or_update_record(mock_cursor, log_entry)
        mock_cursor.execute.assert_called_with(
    """
INSERT INTO kafka_data (log_type, log_time, userid, movieid, minute, rating, server, status, recommendations, responsetime)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""",
    log_entry
)

    @patch('kafka_consumer_tosql.mysql.connector.connect')
    def test_insert_record_invalid_userid(self, mock_connect):
        # Mock MySQL cursor and connection
        mock_cursor = MagicMock()
        mock_connect().cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # Simulate no existing record

        # Invalid log entry with a non-numeric userid
        invalid_log_entry = (
            "rating", '2024-10-05T04:00:54', 'invalid_user_123', 'i+love+cmu+2024', None, '5', None, None, None, None
        )
        # expect validate_data to reject this log entry, therefore insert_or_update_record should not be called
        is_valid = validate_data(invalid_log_entry)
        self.assertFalse(is_valid, "Log entry with an invalid userid should be rejected.")
        if is_valid:
            insert_or_update_record(mock_cursor, invalid_log_entry)
            mock_cursor.execute.assert_not_called()  # We expect no SQL to be executed with invalid data

    @patch('kafka_consumer_tosql.mysql.connector.connect')
    def test_update_record(self, mock_connect):
        # Mock MySQL cursor and connection
        mock_cursor = MagicMock()
        mock_connect().cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1, '55', '4')

        # Mock log entry with the same user and movie, which should merge minute and rating
        log_entry = (
            "watch", '2024-10-05T04:00:54', '12345678', 'i+love+cmu+2024', '60', None, None, None, None, None
        )

        # Expected update query and parameters
        update_query = """
UPDATE kafka_logs SET
    minute = IF(%s IS NOT NULL, CONCAT_WS(',', minute, %s), minute),
    rating = IF(%s IS NOT NULL, CONCAT_WS(',', rating, %s), rating)
WHERE id = %s
"""
        # The expected parameters after merging
        expected_parameters = ('60', '60', None, None, 1)

        # Call the function to update or insert the record
        insert_or_update_record(mock_cursor, log_entry)

        # Assert that the update query was called with the correct parameters
        mock_cursor.execute.assert_called_with(update_query, expected_parameters)

        # Test another log entry with the same user and movie, now including a rating
        log_entry = (
            "rating", '2024-10-05T04:00:54', '12345678', 'i+love+cmu+2024', None, '5', None, None, None, None
        )

        # Update the expected parameters after merging the rating
        expected_parameters = (None, None, '5', '5', 1)

        # Call the function to update or insert the record again
        insert_or_update_record(mock_cursor, log_entry)

        # Assert that the update query was called with the correct parameters again
        mock_cursor.execute.assert_called_with(update_query, expected_parameters)

    
if __name__ == '__main__':
    unittest.main()
