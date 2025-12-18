import threading
import time
import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, call, patch

from xno.data2.technical.external import DataKafkaConsumer, ExternalDataService


class TestDataKafkaConsumer(unittest.TestCase):
    """Unit tests for DataKafkaConsumer"""

    def setUp(self):
        """Set up test fixtures"""
        self.topic = "test_topic"
        self.config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test_group",
            "auto.offset.reset": "earliest",
        }

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    def test_initialization(self, mock_executor_class, mock_consumer_class):
        """Test DataKafkaConsumer initialization"""
        consumer = DataKafkaConsumer(topic=self.topic, max_workers=5, **self.config)

        self.assertEqual(consumer._topic, self.topic)
        self.assertEqual(consumer._max_workers, 5)
        self.assertEqual(consumer._start_config, self.config)
        self.assertIsNone(consumer._consumer)
        self.assertIsNone(consumer._thread)
        self.assertIsNone(consumer._executor)

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    def test_start(self, mock_executor_class, mock_consumer_class):
        """Test starting the consumer"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        consumer = DataKafkaConsumer(topic=self.topic, **self.config)
        consumer.start()

        mock_consumer_class.assert_called_once_with(self.config)
        mock_consumer.subscribe.assert_called_once_with([self.topic])
        mock_executor_class.assert_called_once()
        self.assertIsNotNone(consumer._consumer)
        self.assertIsNotNone(consumer._executor)

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    def test_stop(self, mock_executor_class, mock_consumer_class):
        """Test stopping the consumer"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        consumer = DataKafkaConsumer(topic=self.topic, **self.config)
        consumer.start()
        consumer.stop()

        self.assertTrue(consumer._stop_event.is_set())
        mock_executor.shutdown.assert_called_once_with(wait=True, cancel_futures=False)
        mock_consumer.close.assert_called_once()

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    @patch("xno.data2.technical.external.threading.Thread")
    def test_consume_starts_thread(self, mock_thread_class, mock_executor_class, mock_consumer_class):
        """Test that consume starts a background thread"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor
        mock_thread = MagicMock()
        mock_thread_class.return_value = mock_thread

        callback = MagicMock()
        consumer = DataKafkaConsumer(topic=self.topic, **self.config)
        consumer.start()
        consumer.consume(callback)

        mock_thread_class.assert_called_once()
        mock_thread.start.assert_called_once()

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    def test_t_consume_requires_start(self, mock_executor_class, mock_consumer_class):
        """Test that _t_consume raises error if not started"""
        consumer = DataKafkaConsumer(topic=self.topic, **self.config)

        with self.assertRaises(RuntimeError) as context:
            consumer._t_consume(MagicMock())

        self.assertIn("Consumer not started", str(context.exception))

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    @patch("xno.data2.technical.external.orjson")
    def test_t_consume_processes_messages(self, mock_orjson, mock_executor_class, mock_consumer_class):
        """Test that _t_consume processes messages correctly"""
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        # Mock message
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b'{"data": "test"}'
        mock_consumer.poll.side_effect = [mock_msg, None]

        mock_orjson.loads.return_value = {"data": "test"}

        callback = MagicMock()
        consumer = DataKafkaConsumer(topic=self.topic, **self.config)
        consumer.start()

        def stop_after_first():
            time.sleep(1)
            consumer._stop_event.set()

        threading.Thread(target=stop_after_first).start()

        consumer._t_consume(callback)

        mock_orjson.loads.assert_called_once_with(b'{"data": "test"}')
        mock_executor.submit.assert_called_once_with(callback, {"data": "test"})

    @patch("xno.data2.technical.external.Consumer")
    @patch("xno.data2.technical.external.ThreadPoolExecutor")
    def test_t_consume_handles_kafka_errors(self, mock_executor_class, mock_consumer_class):
        """Test that _t_consume handles Kafka errors gracefully"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        # Mock error message
        from confluent_kafka import KafkaError

        mock_msg = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError._PARTITION_EOF
        mock_msg.error.return_value = mock_error
        mock_consumer.poll.side_effect = [mock_msg, None]

        callback = MagicMock()
        consumer = DataKafkaConsumer(topic=self.topic, **self.config)
        consumer.start()
        consumer._stop_event.set()

        consumer._t_consume(callback)

        # Should not call callback for error messages
        mock_executor.submit.assert_not_called()


class TestExternalDataService(unittest.TestCase):
    """Unit tests for ExternalDataService"""

    def setUp(self):
        """Set up test fixtures"""
        self.consumer_config = {
            "topic": "test_topic",
            "bootstrap.servers": "localhost:9092",
            "group.id": "test_group",
        }
        self.db_name = "test_db"

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_initialization(self, mock_consumer_class):
        """Test ExternalDataService initialization"""
        service = ExternalDataService(self.consumer_config, self.db_name)

        self.assertEqual(service._database_name, self.db_name)
        mock_consumer_class.assert_called_once_with(**self.consumer_config)

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_start(self, mock_consumer_class):
        """Test starting the external data service"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        ohlcv_cb = MagicMock()
        order_book_cb = MagicMock()
        trade_tick_cb = MagicMock()
        quote_tick_cb = MagicMock()
        market_info_cb = MagicMock()
        stock_info_cb = MagicMock()

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.start(
            on_consume_ohlcv=ohlcv_cb,
            on_consume_order_book=order_book_cb,
            on_consume_trade_tick=trade_tick_cb,
            on_consume_quote_tick=quote_tick_cb,
            on_consume_market_info=market_info_cb,
            on_consume_stock_info=stock_info_cb,
        )

        self.assertEqual(service._consumer_ohlcv_callback, ohlcv_cb)
        self.assertEqual(service._consumer_order_book_callback, order_book_cb)
        mock_consumer.start.assert_called_once()
        mock_consumer.consume.assert_called_once()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_on_consume_ohlcv(self, mock_consumer_class):
        """Test consuming OHLCV data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        ohlcv_cb = MagicMock()
        order_book_cb = MagicMock()
        trade_tick_cb = MagicMock()
        quote_tick_cb = MagicMock()
        market_info_cb = MagicMock()
        stock_info_cb = MagicMock()

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.start(
            on_consume_ohlcv=ohlcv_cb,
            on_consume_order_book=order_book_cb,
            on_consume_trade_tick=trade_tick_cb,
            on_consume_quote_tick=quote_tick_cb,
            on_consume_market_info=market_info_cb,
            on_consume_stock_info=stock_info_cb,
        )

        raw = {"data_type": "OH", "symbol": "MSB", "open": 100.0}
        service._on_consume(raw)

        ohlcv_cb.assert_called_once_with(raw)
        order_book_cb.assert_not_called()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_on_consume_order_book(self, mock_consumer_class):
        """Test consuming order book data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        ohlcv_cb = MagicMock()
        order_book_cb = MagicMock()
        trade_tick_cb = MagicMock()
        quote_tick_cb = MagicMock()
        market_info_cb = MagicMock()
        stock_info_cb = MagicMock()

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.start(
            on_consume_ohlcv=ohlcv_cb,
            on_consume_order_book=order_book_cb,
            on_consume_trade_tick=trade_tick_cb,
            on_consume_quote_tick=quote_tick_cb,
            on_consume_market_info=market_info_cb,
            on_consume_stock_info=stock_info_cb,
        )

        raw = {"data_type": "TP", "symbol": "MSB", "bp": [100.0]}
        service._on_consume(raw)

        order_book_cb.assert_called_once_with(raw)
        ohlcv_cb.assert_not_called()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_on_consume_trade_tick(self, mock_consumer_class):
        """Test consuming trade tick data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        ohlcv_cb = MagicMock()
        order_book_cb = MagicMock()
        trade_tick_cb = MagicMock()
        quote_tick_cb = MagicMock()
        market_info_cb = MagicMock()
        stock_info_cb = MagicMock()

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.start(
            on_consume_ohlcv=ohlcv_cb,
            on_consume_order_book=order_book_cb,
            on_consume_trade_tick=trade_tick_cb,
            on_consume_quote_tick=quote_tick_cb,
            on_consume_market_info=market_info_cb,
            on_consume_stock_info=stock_info_cb,
        )

        raw = {"data_type": "ST", "symbol": "MSB", "price": 100.0}
        service._on_consume(raw)

        trade_tick_cb.assert_called_once_with(raw)
        ohlcv_cb.assert_not_called()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_on_consume_market_info(self, mock_consumer_class):
        """Test consuming market info data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        ohlcv_cb = MagicMock()
        order_book_cb = MagicMock()
        trade_tick_cb = MagicMock()
        quote_tick_cb = MagicMock()
        market_info_cb = MagicMock()
        stock_info_cb = MagicMock()

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.start(
            on_consume_ohlcv=ohlcv_cb,
            on_consume_order_book=order_book_cb,
            on_consume_trade_tick=trade_tick_cb,
            on_consume_quote_tick=quote_tick_cb,
            on_consume_market_info=market_info_cb,
            on_consume_stock_info=stock_info_cb,
        )

        raw = {"data_type": "MI", "symbol": "VNINDEX"}
        service._on_consume(raw)

        market_info_cb.assert_called_once_with(raw)

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_on_consume_stock_info(self, mock_consumer_class):
        """Test consuming stock info data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        ohlcv_cb = MagicMock()
        order_book_cb = MagicMock()
        trade_tick_cb = MagicMock()
        quote_tick_cb = MagicMock()
        market_info_cb = MagicMock()
        stock_info_cb = MagicMock()

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.start(
            on_consume_ohlcv=ohlcv_cb,
            on_consume_order_book=order_book_cb,
            on_consume_trade_tick=trade_tick_cb,
            on_consume_quote_tick=quote_tick_cb,
            on_consume_market_info=market_info_cb,
            on_consume_stock_info=stock_info_cb,
        )

        raw = {"data_type": "SI", "symbol": "MSB"}
        service._on_consume(raw)

        stock_info_cb.assert_called_once_with(raw)

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_ohlcv(self, mock_sql_session, mock_consumer_class):
        """Test getting historical OHLCV data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        # Mock SQL session and results
        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_rows = [
            (datetime(2024, 12, 18, 10, 0, 0), 100.0, 105.0, 99.0, 103.0, 1000000.0),
        ]
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        result = service.get_history_ohlcv(
            symbol="MSB",
            resolution="DAY",
            from_time=datetime(2024, 12, 18),
            to_time=datetime(2024, 12, 19),
        )

        self.assertEqual(result, mock_rows)
        mock_session.execute.assert_called_once()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_ohlcv_with_limit(self, mock_sql_session, mock_consumer_class):
        """Test getting historical OHLCV data with limit"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.get_history_ohlcv(symbol="MSB", resolution="DAY", limit=100)

        # Verify SQL includes LIMIT
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        self.assertIn("LIMIT", sql_text)

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_order_book_depth(self, mock_sql_session, mock_consumer_class):
        """Test getting historical order book depth data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_rows = [
            (datetime(2024, 12, 18, 10, 0, 0), "MSB", [100.0], [1000], [101.0], [1500], 1000, 1500),
        ]
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        result = service.get_history_order_book_depth(
            symbol="MSB",
            from_time=datetime(2024, 12, 18),
            to_time=datetime(2024, 12, 19),
        )

        self.assertEqual(result, mock_rows)
        mock_session.execute.assert_called_once()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_order_book_depth_with_resolution(self, mock_sql_session, mock_consumer_class):
        """Test getting resampled order book depth data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.get_history_order_book_depth(symbol="MSB", resolution="1H")

        # Verify SQL includes time_bucket
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        self.assertIn("time_bucket", sql_text)

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_trade_tick(self, mock_sql_session, mock_consumer_class):
        """Test getting historical trade tick data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_rows = [
            (datetime(2024, 12, 18, 10, 0, 0), "MSB", 100.0, 1000, "BUY", "dnse"),
        ]
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        result = service.get_history_trade_tick(
            symbol="MSB",
            from_time=datetime(2024, 12, 18),
            to_time=datetime(2024, 12, 19),
        )

        self.assertEqual(result, mock_rows)

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_market_info(self, mock_sql_session, mock_consumer_class):
        """Test getting historical market info data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.get_history_market_info(symbol="VNINDEX")

        mock_session.execute.assert_called_once()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_stock_info(self, mock_sql_session, mock_consumer_class):
        """Test getting historical stock info data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.get_history_stock_info(symbol="MSB")

        mock_session.execute.assert_called_once()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    @patch("xno.data2.technical.external.SqlSession")
    def test_get_history_stock_price_board(self, mock_sql_session, mock_consumer_class):
        """Test getting historical stock price board data"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_session = MagicMock()
        mock_sql_session.return_value.__enter__.return_value = mock_session
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.get_history_stock_price_board(symbol="MSB")

        mock_session.execute.assert_called_once()

    @patch("xno.data2.technical.external.DataKafkaConsumer")
    def test_stop(self, mock_consumer_class):
        """Test stopping the service"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        service = ExternalDataService(self.consumer_config, self.db_name)
        service.stop()

        mock_consumer.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
