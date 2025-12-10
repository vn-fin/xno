import logging
import pyarrow as pa
from datetime import datetime
from xno.data2.data.db import flush_arrow_to_duckdb, fetch_numpy

logger = logging.getLogger(__name__)
SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("resolution", pa.string()),
    ("time", pa.int64()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
])


def get(symbol: str, resolution: str, from_time: str, to_time: str, table_name="ohlcv_data"):
    sql = f"""
       SELECT *
       FROM {table_name}
       WHERE symbol = '{symbol}'
         AND resolution = '{resolution}'
       """

    if from_time:
        from_time = datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1_000
        sql += f" AND time > {from_time}"
    if to_time:
        to_time = datetime.strptime(to_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1_000
        sql += f" AND time < {to_time}"

    data = fetch_numpy(sql=sql)
    return data

def push(symbol, resolution, raws):
    sync_from_external_db(symbol, resolution, raws)

def parse_from_external_kafka(raw):
    # {'time': 1765332000, 'symbol': 'C4G', 'resolution': 'DAY', 'open': 8.5, 'high': 8.8, 'low': 8.4, 'close': 8.6, 'volume': 3156600.0, 'updated': 1765352009, 'data_type': 'OH', 'source': 'dnse'}
    symbol, resolution = raw['symbol'], raw['resolution']
    data = [raw['time'] * 1_000, raw['open'], raw['high'], raw['low'], raw['close'], raw['volume']]

    return symbol, resolution, data

def sync_from_external_db(symbol, resolution, raws, table="ohlcv_data"):
    print(f"Syncing OHLCV data to DuckDB table {table}")
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([symbol for _ in range(0, len(raws))], pa.string()),
            pa.array([resolution for _ in range(0, len(raws))], pa.string()),
            pa.array([r[0] if isinstance(r[0], int) else r[0].timestamp() * 1_000 for r in raws], pa.int64()),
            pa.array([float(r[1]) for r in raws], pa.float64()),
            pa.array([float(r[2]) for r in raws], pa.float64()),
            pa.array([float(r[3]) for r in raws], pa.float64()),
            pa.array([float(r[4]) for r in raws], pa.float64()),
            pa.array([float(r[5]) for r in raws], pa.float64()),
        ],
        schema=SCHEMA
    )
    flush_arrow_to_duckdb(batch, table_name=table)
