import pytest
from unittest.mock import patch, MagicMock

import src.producer as producer

def test_producer_sends_message_to_kafka():
    mock_kafka_producer = MagicMock()
    test_tick = {
        "symbol": "AAPL",
        "price": 150.0,
        "volume": 100,
        "timestamp": "2025-07-19T10:00:00Z"
    }
    with patch('src.producer.KafkaProducer', return_value=mock_kafka_producer):
        with patch('src.producer.get_tick_data', return_value=[test_tick]):
            producer.run_producer()
            mock_kafka_producer.send.assert_called()