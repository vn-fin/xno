import logging
from datetime import datetime

from xno.data2.entity import OHLCV, OHLCVs
from xno.data2.store.db import connect, execute, fetch_numpy

logger = logging.getLogger(__name__)

__ohlcv_db_name = "ohlcv_data"


def init():
    create_db_table()


def create_db_table():
    sql = f"""
    CREATE TABLE IF NOT EXISTS {__ohlcv_db_name} (
        symbol STRING NOT NULL,
        resolution STRING NOT NULL,
        time TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        PRIMARY KEY (symbol, resolution, time)
    )
    """
    execute(sql)


def get_numpy(
    symbol: str,
    resolution: str,
    from_time: datetime,
    to_time: datetime,
) -> dict:
    sql = f"""
       SELECT *
       FROM {__ohlcv_db_name}
       WHERE symbol = $symbol
         AND resolution = $resolution
       """
    params = {"symbol": symbol, "resolution": resolution}
    if from_time:
        sql += " AND time >= $from_time"
        params["from_time"] = from_time
    if to_time:
        sql += " AND time <= $to_time"
        params["to_time"] = to_time

    sql += " ORDER BY time ASC"

    data = fetch_numpy(sql=sql, params=params)
    return data


def push(symbol, resolution, ohlcv: OHLCV):
    row = ohlcv.to_duckdb_row(symbol=symbol, resolution=resolution)
    sql = f"INSERT OR REPLACE INTO {__ohlcv_db_name} (symbol, resolution, time, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?,?)"
    execute(sql, row)


def pushes(ohlcvs: OHLCVs):
    if __debug__:
        logger.debug("Pushing %s OHLCV records to DuckDB", len(ohlcvs))

    # TODO: replace with duckdb appender when available
    batch = ohlcvs.to_pyarrow_batch()
    connect().execute(f"INSERT OR REPLACE INTO {__ohlcv_db_name} SELECT DISTINCT * FROM batch")
