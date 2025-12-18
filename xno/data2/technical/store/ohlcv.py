import logging
import threading
import time
from datetime import datetime

import polars as pl

from xno.data2.technical.entity import OHLCV, OHLCVs
from xno.data2.technical.entity.ohlcv import POLARS_SCHEMA as OHLCV_POLARS_SCHEMA
from xno.data2.technical.store.db import connect

logger = logging.getLogger(__name__)


_LAST_OHLCV: dict[str, OHLCV] = {}
_OHLCV_HISTORY_DATA: dict[str, pl.DataFrame] = {}
_OHLCV_BUFFER_DATA: dict[str, dict[str, dict]] = {}

_stop_event = threading.Event()


def start():
    threading.Thread(target=_t_flush_buffers, daemon=True).start()


def stop():
    _stop_event.set()


def _t_flush_buffers():
    while not _stop_event.is_set():
        time.sleep(30)
        flush_buffers()


def flush_buffers():
    global _OHLCV_BUFFER_DATA

    buffers = _OHLCV_BUFFER_DATA
    _OHLCV_BUFFER_DATA = {}

    for symbol, resolution in buffers.keys():
        if not (symbol, resolution) in _OHLCV_HISTORY_DATA:
            continue

        buffer = list(buffers[(symbol, resolution)].values())
        buffer_df = pl.from_dicts(buffer, schema=OHLCV_POLARS_SCHEMA)

        polars_df = _OHLCV_HISTORY_DATA[(symbol, resolution)]
        polars_df = pl.concat([polars_df, buffer_df]).unique(["time"], keep="last").sort("time")
        _OHLCV_HISTORY_DATA[(symbol, resolution)] = polars_df


def _is_come_late_ohlcv(symbol: str, resolution: str, ohlcv: OHLCV):
    """
    Check if the incoming OHLCV data is late (duplicate or out-of-order)
    """
    key = f"{symbol}_{resolution}_{ohlcv.time.timestamp()}"
    if key in _LAST_OHLCV:
        last_ohlcv = _LAST_OHLCV[key]
        if ohlcv.time == last_ohlcv.time and ohlcv.volume < last_ohlcv.volume:
            if __debug__:
                logger.debug("Come late OHLCV data ignored: %s %s %s != %s", symbol, resolution, last_ohlcv, ohlcv)
            return True

    _LAST_OHLCV[key] = ohlcv
    return False


def get_numpy(
    symbol: str,
    resolution: str,
    from_time: datetime,
    to_time: datetime,
) -> dict:
    flush_buffers()

    history_df = _OHLCV_HISTORY_DATA.get((symbol, resolution), None)
    if history_df is None:
        return pl.DataFrame([], schema=OHLCV_POLARS_SCHEMA).to_numpy()

    # history_df = history_df.filter(pl.col("time") >= from_time, pl.col("time") <= to_time)
    # return history_df.to_numpy()

    sql = """
    SELECT time, open, high, low, close, volume
    FROM history_df
    WHERE time >= $from_time AND time <= $to_time
    ORDER BY time ASC
    """

    return connect().execute(sql, {"from_time": from_time, "to_time": to_time}).fetchnumpy()


def push(symbol, resolution, ohlcv: OHLCV):
    if _is_come_late_ohlcv(symbol, resolution, ohlcv):
        return

    buffer = _OHLCV_BUFFER_DATA.setdefault((symbol, resolution), {})
    buffer[ohlcv.time] = ohlcv.to_dict()
    _OHLCV_BUFFER_DATA[(symbol, resolution)] = buffer


def pushes(ohlcvs: OHLCVs, check_come_late: bool = False):
    if __debug__:
        logger.debug("Pushing %s OHLCV records for %s %s to DuckDB", len(ohlcvs), ohlcvs.symbol, ohlcvs.resolution)

    # if check_come_late:
    #     valid_ohlcvs = []
    #     for ohlcv in ohlcvs.ohlcvs:
    #         if not _is_come_late_ohlcv(ohlcvs.symbol, ohlcvs.resolution, ohlcv):
    #             valid_ohlcvs.append(ohlcv)
    #     ohlcvs = OHLCVs(symbol=ohlcvs.symbol, resolution=ohlcvs.resolution, ohlcvs=valid_ohlcvs)

    df = ohlcvs.to_polars_df().unique(["time"], keep="last").sort("time")
    _OHLCV_HISTORY_DATA[(ohlcvs.symbol, ohlcvs.resolution)] = df

    if __debug__:
        logger.debug("Pushed %s OHLCV records for %s %s to DuckDB", len(ohlcvs), ohlcvs.symbol, ohlcvs.resolution)
