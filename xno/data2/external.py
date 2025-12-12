import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy import text

from xno.connectors.sql import SqlSession

logger = logging.getLogger(__name__)


class DataKafkaConsumer:
    """
    Consume OHLCV/OrderBook messages from Confluent Kafka.
    """

    def __init__(self, topic: str, **kwargs) -> None:
        self._start_config = kwargs
        self._topic = topic

        self._consumer: Optional[Consumer] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start consuming messages from Kafka."""
        if __debug__:
            logger.debug(f"Starting Kafka consumer for topic {self._topic} with config {self._start_config}")

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
        self._thread = threading.Thread(target=self._t_consume, name="DataKafkaConsumer", daemon=True, args=(callback,))
        self._thread.start()

    def _t_consume(self, callback: Callable) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer not started. Call start() before consuming.")

        if __debug__:
            logger.debug(f"Consuming data for {self._topic}")
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

                try:
                    raw = orjson.loads(raw)

                    # TODO: limit threading count
                    threading.Thread(target=callback, args=(raw,), daemon=True).start()
                except Exception as e:
                    logger.error("Error processing message: %s", raw, exc_info=True)
        finally:
            try:
                logger.warning("Closing Kafka consumer for topic %s", self._topic)
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
        self._consumer_trade_tick_callback = None
        self._consumer_quote_tick_callback = None

    def start(
        self,
        on_consume_ohlcv: Callable,
        on_consume_order_book: Callable,
        on_consume_trade_tick: Callable,
        on_consume_quote_tick: Callable,
    ):
        self._consumer_ohlcv_callback = on_consume_ohlcv
        self._consumer_order_book_callback = on_consume_order_book
        self._consumer_trade_tick_callback = on_consume_trade_tick
        self._consumer_quote_tick_callback = on_consume_quote_tick

        self._data_consumer.start()
        self._data_consumer.consume(self._on_consume)

    def _on_consume(self, raw):
        try:
            # if __debug__:
            #     logger.debug("Consumed message: %s", raw)
            match raw["data_type"]:
                case "OH":
                    self._consumer_ohlcv_callback(raw)
                case "TP":
                    self._consumer_order_book_callback(raw)
                case "ST":
                    self._consumer_trade_tick_callback(raw)
                case "SF":
                    self._consumer_quote_tick_callback(raw)
        except Exception as e:
            logger.error("Error processing consumed message: %s", raw, exc_info=True)
            raise e

    def get_history_ohlcv(self, symbol: str, resolution: str, from_time=None, to_time=None):
        if __debug__:
            logger.debug(f"Getting OHLCV data for {symbol} at {resolution} resolution from {from_time} to {to_time}")

        with SqlSession(self._database_name) as session:
            sql = """
                  SELECT time, open, high, low, close, volume
                  FROM vn_market.history_stock_ohlcv
                  WHERE symbol = :symbol AND resolution = :resolution \
                  """
            if from_time is not None:
                sql += " AND time >= :from_time"
            if to_time is not None:
                sql += " AND time <= :to_time"

            result = session.execute(
                text(sql), dict(symbol=symbol, resolution=resolution, from_time=from_time, to_time=to_time)
            )
            rows = result.fetchall()

            if __debug__:
                logger.debug(f"Loaded {len(rows)} rows from DB for {symbol} at {resolution} resolution")
        return rows

    def get_history_order_book_depths(
        self,
        symbol: str,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ):
        if __debug__:
            logger.debug(f"Getting depths for {symbol} since {from_time} to {to_time}")

        with SqlSession(self._database_name) as session:
            sql = """
                  SELECT time, symbol, bp, bq, ap, aq, total_bid, total_ask
                  FROM vn_market.history_stock_top_price
                  WHERE symbol = :symbol \
                  """
            if from_time is not None:
                sql += " AND time >= :from_time"
            if to_time is not None:
                sql += " AND time <= :to_time"

            sql += " ORDER BY time ASC"

            result = session.execute(text(sql), dict(symbol=symbol, from_time=from_time, to_time=to_time))
            rows = result.fetchall()

            if __debug__:
                logger.debug(f"Loaded {len(rows)} rows from DB for {symbol} Order Book data")
        return rows

    def stop(self):
        self._data_consumer.stop()
