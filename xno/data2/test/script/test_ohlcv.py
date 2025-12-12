import logging
import os
import random
import sys
import threading
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))
load_dotenv("../xalpha/.env")

logging.basicConfig(level=logging.DEBUG)

from xno.data2 import DataProvider
from xno.utils.dc import timing

_close_event = threading.Event()
_total_requests = 0


def _t_request_ohlcv(all_data, from_time, to_time):
    global _total_requests
    while not _close_event.is_set():
        symbol, resolution = random.choice(all_data)

        process_id = _total_requests
        _total_requests += 1

        print(f"---> [{process_id}] Requesting OHLCV for {symbol} {resolution}...")
        ohlcvs = _process_ohlcv_request(symbol, resolution, from_time, to_time)
        print(f"===> [{process_id}] Fetched {len(ohlcvs)} OHLCV records for {symbol} {resolution}")


@timing
def _process_ohlcv_request(symbol, resolution, from_time, to_time):
    ohlcvs = DataProvider.get_ohlcv(
        symbol=symbol,
        resolution=resolution,
        from_time=from_time,
        to_time=to_time,
    )
    return ohlcvs


def benchmark_get_ohlcv():
    all_data = []
    for symbol in (
        "ACB",
        "ANV",
        "BCM",
        "BID",
        "BMP",
        "BSI",
        "BVH",
        "BWE",
        "CII",
        "CMG",
        "CTD",
        "CTG",
        "CTR",
        "CTS",
        "DBC",
        "DCM",
        "DGC",
        "DGW",
        "DIG",
        "DPM",
        "DSE",
        "DXG",
        "DXS",
        "EIB",
        "EVF",
        "FPT",
        "FRT",
        "FTS",
        "GAS",
        "GEE",
        "GEX",
        "GMD",
        "GVR",
        "HAG",
        "HCM",
        "HDB",
        "HDC",
        "HDG",
        "HHV",
        "HPG",
        "HSG",
        "HT1",
        "IMP",
        "KBC",
        "KDC",
        "KDH",
        "KOS",
        "LPB",
        "MBB",
        "MSB",
        "MSN",
        "MWG",
        "NAB",
        "NKG",
        "NLG",
        "NT2",
        "OCB",
        "PAN",
        "PC1",
        "PDR",
        "PHR",
        "PLX",
        "PNJ",
        "POW",
        "PPC",
        "PTB",
        "PVD",
        "PVT",
        "REE",
        "SAB",
        "SBT",
        "SCS",
        "SHB",
        "SIP",
        "SJS",
        "SSB",
        "SSI",
        "STB",
        "SZC",
        "TCB",
        "TCH",
        "TLG",
        "TPB",
        "VCB",
        "VCG",
        "VCI",
        "VGC",
        "VHC",
        "VHM",
        "VIB",
        "VIC",
        "VIX",
        "VJC",
        "VND",
        "VNM",
        "VPB",
        "VPI",
        "VRE",
        "VSC",
        "VTP",
    ):
        for resolution in ("MIN", "HOUR1", "DAY"):
            all_data.append((symbol, resolution))

    for _ in range(30):
        threading.Thread(
            target=_t_request_ohlcv, args=(all_data, datetime.now() - timedelta(weeks=52), datetime.now())
        ).start()


def test_ohlcv_union():
    print("Inserting OHLCV data into buffer...")
    DataProvider._on_consume_ohlcv(
        {
            "symbol": "ACB",
            "resolution": "MIN",
            "time": int(datetime.now().timestamp()),
            "open": 100,
            "high": 200,
            "low": 50,
            "close": 150,
            "volume": 1000,
        }
    )

    print("Fetching OHLCV data including buffer...")
    data = DataProvider.get_ohlcv(
        symbol="ACB",
        resolution="MIN",
        from_time=datetime.now() - timedelta(days=1),
        to_time=datetime.now(),
    )
    print("Fetched", len(data))
    print(data)


def monitor_ohlcv_realtime_update():
    data = DataProvider.get_ohlcv(
        symbol="ACB",
        resolution="MIN",
        from_time=datetime.now() - timedelta(days=60),
        to_time=datetime.now(),
    )
    print(data)

    for _ in range(30):
        time.sleep(10)
        data = DataProvider.get_ohlcv(
            symbol="ACB",
            resolution="MIN",
            from_time=datetime.now() - timedelta(days=60),
            to_time=datetime.now(),
        )
        print(data)


if __name__ == "__main__":
    try:
        DataProvider.start()

        benchmark_get_ohlcv()
        # monitor_ohlcv_realtime_update()
        # test_ohlcv_union()

        time.sleep(600)
    finally:
        _close_event.set()
        DataProvider.stop()

    print("Data provider stopped.")
