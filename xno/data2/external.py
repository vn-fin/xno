import json
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Callable
from sqlalchemy import text
from confluent_kafka import Consumer, KafkaError, KafkaException
from websockets import Data

from xno.connectors.sql import SqlSession

logger = logging.getLogger(__name__)


class DataKafkaConsumer:
    """
    Consume OHLCV messages from Confluent Kafka and save to DuckDB.

    Example:
        consumer = KafkaOHLCVConsumer(
            bootstrap_servers="localhost:9092",
            topic="ohlcv",
            group_id="xno-ohlcv",
            table="ohlcv",
        )
        consumer.start()
        ...
        consumer.stop()
    """

    def __init__(self, topic: str, **kwargs) -> None:
        self._start_config = kwargs
        self._topic = topic

        self._consumer: Optional[Consumer] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        self._consumer = Consumer(self._start_config)
        self._consumer.subscribe([self._topic])
        self._stop_event.clear()

    def stop(self) -> None:
        """Stop consuming and close the consumer."""
        self._stop_event.set()
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def consume(self, callback: Callable) -> None:
        """Register a callback to be called on each consumed message."""
        self._thread = threading.Thread(target=self._t_consume, name="KafkaOHLCVConsumer", daemon=True,
                                        args=(callback,))
        self._thread.start()

    def _t_consume(self, callback: Callable) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer not started. Call start() before consuming.")

        print(f"Consuming OHLCV data for {self._topic}")
        try:
            while not self._stop_event.is_set():
                try:
                    msg = self._consumer.poll(timeout=1.0)
                except KafkaException as ke:
                    logger.exception("Kafka poll error: %s", ke)
                    time.sleep(1.0)
                    continue
                except Exception as exc:
                    logger.exception("Unexpected poll error: %s", exc)
                    time.sleep(1.0)
                    continue

                if msg is None:
                    continue

                if msg.error():
                    # handle non-fatal errors
                    err = msg.error()
                    # ignore EOF for partitions or log transient errors
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka message error: %s", err)
                    continue

                # decode message value
                raw = msg.value()
                callback(json.loads(raw))
        finally:
            try:
                if self._consumer is not None:
                    self._consumer.close()
            except Exception:
                pass


class ExternalDataService:
    def __init__(self, consumer_config: Dict[str, Any], db_name: str = "xno_data"):
        self._database_name = db_name
        self._data_consumer = DataKafkaConsumer(**consumer_config)
        self._consumer_ohlcv_callback = None
        self._consumer_order_book_callback = None

    def start(self, on_consume_ohlcv: Callable, on_consume_order_book: Callable):
        self._consumer_ohlcv_callback = on_consume_ohlcv
        self._consumer_order_book_callback = on_consume_order_book

        self._data_consumer.start()
        self._data_consumer.consume(self._on_consume)

    def _on_consume(self, raw):
        if __debug__:
            if not self._consumer_ohlcv_callback:
                raise RuntimeError("Consumer not started. Call start() before consuming.")
            if not self._consumer_order_book_callback:
                raise RuntimeError("Consumer not started. Call start() before consuming.")

        match raw['data_type']:
            case "OH":
                self._consumer_ohlcv_callback(raw)
            case "TP":
                self._consumer_order_book_callback(raw)

    def get_ohlcv(self, symbol: str, resolution: str, from_time=None, to_time=None):
        print(f"Getting OHLCV data from DB for {symbol} at {resolution} resolution from database {self._database_name}")

        with SqlSession(self._database_name) as session:
            query = """
                    SELECT time, open, high, low, close, volume
                    FROM vn_market.history_stock_ohlcv
                    WHERE symbol = :symbol AND resolution = :resolution \
                    """
            if from_time is not None:
                query += " AND time >= :from_time"
            if to_time is not None:
                query += " AND time <= :to_time"
            result = session.execute(text(query), dict(
                symbol=symbol,
                resolution=resolution,
                from_time=from_time,
                to_time=to_time
            ))
            rows = result.fetchall()
            print(f"Loaded {len(rows)} rows from DB for {symbol} at {resolution} resolution")
        return rows

    def get_order_book_depths(self, symbol: str, from_time: datetime | None = None, to_time: datetime | None = None):
        print(f"Getting Order Book data from DB for {symbol} from database {self._database_name}")

        with SqlSession(self._database_name) as session:
            query = """
                    SELECT time, symbol, bp, bq, ap, aq, total_bid, total_ask
                    FROM vn_market.history_stock_top_price
                    WHERE symbol = :symbol \
                    """
            if from_time is not None:
                query += " AND time >= :from_time"
            if to_time is not None:
                query += " AND time <= :to_time"
            result = session.execute(text(query), dict(
                symbol=symbol,
                from_time=from_time,
                to_time=to_time
            ))
            rows = result.fetchall()
            print(f"Loaded {len(rows)} rows from DB for {symbol} Order Book data")
        return rows

    def stop(self):
        self._data_consumer.stop()
