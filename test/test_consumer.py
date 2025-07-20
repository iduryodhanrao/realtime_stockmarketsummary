import pytest
from unittest.mock import patch, MagicMock

import src.consumer as consumer

def test_consumer_aggregates_and_saves_to_db():
    mock_kafka_consumer = MagicMock()
    mock_db_conn = MagicMock()
    test_ticks = [
        {"symbol": "AAPL", "price": 150.0, "volume": 100, "timestamp": "2025-07-19T10:00:00Z"},
        {"symbol": "AAPL", "price": 151.0, "volume": 50, "timestamp": "2025-07-19T10:00:30Z"},
        {"symbol": "AAPL", "price": 149.5, "volume": 75, "timestamp": "2025-07-19T10:00:59Z"},
    ]
    with patch('src.consumer.KafkaConsumer', return_value=mock_kafka_consumer):
        with patch('src.consumer.connect_db', return_value=mock_db_conn):
            with patch('src.consumer.get_ticks_from_kafka', return_value=test_ticks):
                consumer.run_consumer()
                mock_db_conn.save_ohlcv.assert_called()