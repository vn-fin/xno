import numpy as np
import numpy.typing as npt

from typing import TYPE_CHECKING

from xno.data2.docs import document, DOCUMENT_GROUP_L2
from xno.data2.technical.provider import TechnicalDataProvider

if TYPE_CHECKING:
    from xno.data2.technical.entity import OrderBookDepth


class TechnicalL2DataProvider:
    def __init__(self, symbol: str, resolution: str, from_time: str, to_time: str):
        self._symbol = symbol
        self._resolution = resolution
        self._from_time = from_time
        self._to_time = to_time
        self._data_provider = TechnicalDataProvider.singleton()

    def get_historical_order_book_depth(self, depth: int = 10) -> "list[OrderBookDepth] | None":
        """Fetch historical order book depth data at a specific time up to specified depth level"""
        return self._data_provider.get_history_order_book_depth(
            symbol=self._symbol,
            resolution=self._resolution,
            from_time=self._from_time,
            to_time=self._to_time,
            depth=depth,
        )

    def load_data(self):
        """Load historical L2 data (if needed)"""
        # This method can be expanded to preload or cache historical data if necessary
        self._datas = self.get_historical_order_book_depth()
        if __debug__:
            print(f"Loaded historical L2 data for {self._symbol} from {self._from_time} to {self._to_time}")


    @document(
        group=DOCUMENT_GROUP_L2,
        name="mid_price",
        prototype="self.l2.mid_price -> npt.NDArray[np.float64 | None]",
        docs="Midpoint between best bid & ask",
    )
    @property
    def mid_price(self) -> npt.NDArray[np.float64 | None]:
        """Midpoint between best bid & ask"""
        if not self._datas:
            return np.empty(0)
        return np.array([self._mid_price(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _mid_price(self, data) -> np.float64 | None:
        """Midpoint between best bid & ask"""
        if data is None:
            return None

        best_bid = data["bp"][0] if data["bp"] else None
        best_ask = data["ap"][0] if data["ap"] else None

        if best_bid is not None and best_ask is not None:
            return (best_bid + best_ask) / 2

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="spread_abs",
        prototype="self.l2.spread_abs -> npt.NDArray[np.float64 | None]",
        docs="Absolute spread",
    )
    @property
    def spread_abs(self) -> npt.NDArray[np.float64 | None]:
        """Absolute spread"""
        if not self._datas:
            return np.empty(0)
        return np.array([self._spread_abs(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _spread_abs(self, data) -> np.float64 | None:
        """Absolute spread"""
        if data is None:
            return None

        best_bid = data["bp"][0] if data["bp"] else None
        best_ask = data["ap"][0] if data["ap"] else None

        if best_bid is not None and best_ask is not None:
            return best_ask - best_bid

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="spread_rel",
        prototype="self.l2.spread_rel -> npt.NDArray[np.float64 | None]",
        docs="Spread normalized by mid",
    )
    @property
    def spread_rel(self) -> npt.NDArray[np.float64 | None]:
        """Spread normalized by mid"""
        if not self._datas:
            return np.empty(0)
        return np.array([self._spread_rel(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _spread_rel(self, data) -> np.float64 | None:
        """Spread normalized by mid"""
        if data is None:
            return None

        best_bid = data["bp"][0] if data["bp"] else None
        best_ask = data["ap"][0] if data["ap"] else None
        mid_price = self._mid_price(data)

        if best_bid is not None and best_ask is not None and mid_price != 0:
            return (best_ask - best_bid) / mid_price

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="obi_l1",
        prototype="self.l2.obi_l1 -> npt.NDArray[np.float64 | None]",
        docs="Order Book Imbalance at Level 1",
    )
    @property
    def obi_l1(self) -> npt.NDArray[np.float64 | None]:
        """Order Book Imbalance at Level 1"""
        if not self._datas:
            return np.empty(0)
        return np.array([self._obi_l1(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _obi_l1(self, data) -> np.float64 | None:
        """Order Book Imbalance at Level 1"""
        if data is None:
            return None

        bid_size = data["bs"][0] if data["bs"] else None
        ask_size = data["as"][0] if data["as"] else None

        if bid_size is not None and ask_size is not None and (bid_size + ask_size) != 0:
            return (bid_size - ask_size) / (bid_size + ask_size)

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="obi_depth",
        prototype="self.l2.obi_depth(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Multi-level imbalance",
    )
    @property
    def obi_depth(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Order Book Imbalance at specified depth level"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._obi_depth(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _obi_depth(self, data, depth_level: int = 10) -> np.float64 | None:
        """Order Book Imbalance at specified depth level"""
        if data is None:
            return None

        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0

        if (total_bid_size + total_ask_size) != 0:
            return (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size)

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_bid_total",
        prototype="self.l2.depth_bid_total(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Total bid size across all depth levels",
    )
    @property
    def depth_bid_total(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Total bid size up to specified depth level"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_bid_total(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_bid_total(self, data, depth_level: int = 10) -> np.float64 | None:
        """Total bid size up to specified depth level"""
        if data is None:
            return None

        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        return total_bid_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_ask_total",
        prototype="self.l2.depth_ask_total(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Total ask size across all depth levels",
    )
    @property
    def depth_ask_total(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Total ask size up to specified depth level"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_ask_total(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_ask_total(self, data, depth_level: int = 10) -> np.float64 | None:
        """Total ask size up to specified depth level"""
        if data is None:
            return None

        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0
        return total_ask_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_imbalance",
        prototype="self.l2.depth_imbalance(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Buy vs sell pressure across depth",
    )
    @property
    def depth_imbalance(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Buy vs sell pressure across depth"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_imbalance(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_imbalance(self, data, depth_level: int = 10) -> np.float64 | None:
        """Buy vs sell pressure across depth"""
        if data is None:
            return None

        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0

        if (total_bid_size + total_ask_size) != 0:
            return (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size)

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_delta",
        prototype="self.l2.depth_delta(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Net bid - ask depth volume",
    )
    @property
    def depth_delta(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Net bid - ask depth volume"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_delta(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_delta(self, data, depth_level: int = 10) -> np.float64 | None:
        """Net bid - ask depth volume"""
        if data is None:
            return None

        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0

        return total_bid_size - total_ask_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="volume_at_touch",
        prototype="self.l2.volume_at_touch -> npt.NDArray[np.float64 | None]",
        docs="Liquidity at best bid/ask",
    )
    @property
    def volume_at_touch(self) -> npt.NDArray[np.float64 | None]:
        """Liquidity at best bid/ask"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._volume_at_touch(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _volume_at_touch(self, data, depth_level: int = 10) -> np.float64 | None:
        """Liquidity at best bid/ask"""
        if data is None:
            return None

        best_bid_size = data["bs"][0] if data["bs"] else None
        best_ask_size = data["as"][0] if data["as"] else None

        return best_bid_size + best_ask_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="volume_behind",
        prototype="self.l2.volume_behind(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Liquidity away from touch",
    )
    @property
    def volume_behind(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Liquidity away from touch"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._volume_behind(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _volume_behind(self, data, depth_level: int = 10) -> np.float64 | None:
        """Liquidity away from touch"""
        if data is None:
            return None

        best_bid = data["bp"][0] if data["bp"] else None
        best_ask = data["ap"][0] if data["ap"] else None
        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0

        return (total_bid_size + total_ask_size) - (best_bid + best_ask)

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_weighted_mid_price",
        prototype="self.l2.depth_weighted_mid_price(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Size-weighted mid price across full depth",
    )
    @property
    def depth_weighted_mid_price(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Size-weighted mid price across full depth"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_weighted_mid_price(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_weighted_mid_price(self, data, depth_level: int = 10) -> np.float64 | None:
        """Size-weighted mid price across full depth"""
        if data is None:
            return None

        weighted_bid_sum = (
            sum(price * size for price, size in zip(data["bp"][:depth_level], data["bs"][:depth_level]))
            if data["bp"] and data["bs"]
            else 0
        )
        weighted_ask_sum = (
            sum(price * size for price, size in zip(data["ap"][:depth_level], data["as"][:depth_level]))
            if data["ap"] and data["as"]
            else 0
        )
        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0

        if total_bid_size == 0 or total_ask_size == 0:
            return None

        return (weighted_bid_sum + weighted_ask_sum) / (total_bid_size + total_ask_size)

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_weighted_bid_price",
        prototype="self.l2.depth_weighted_bid_price(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Weighted bid price",
    )
    @property
    def depth_weighted_bid_price(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Weighted bid price"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_weighted_bid_price(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_weighted_bid_price(self, data, depth_level: int = 10) -> np.float64 | None:
        """Weighted bid price"""
        if data is None:
            return None

        weighted_bid_sum = (
            sum(price * size for price, size in zip(data["bp"][:depth_level], data["bs"][:depth_level]))
            if data["bp"] and data["bs"]
            else 0
        )
        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0

        if total_bid_size == 0:
            return None

        return weighted_bid_sum / total_bid_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_weighted_ask_price",
        prototype="self.l2.depth_weighted_ask_price(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Weighted ask price",
    )
    @property
    def depth_weighted_ask_price(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Weighted ask price"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_weighted_ask_price(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_weighted_ask_price(self, data, depth_level: int = 10) -> np.float64 | None:
        """Weighted ask price"""
        if data is None:
            return None

        weighted_ask_sum = (
            sum(price * size for price, size in zip(data["ap"][:depth_level], data["as"][:depth_level]))
            if data["ap"] and data["as"]
            else 0
        )
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0

        if total_ask_size == 0:
            return None

        return weighted_ask_sum / total_ask_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_mid_spread",
        prototype="self.l2.depth_mid_spread(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Difference between weighted mid and top mid",
    )
    @property
    def depth_mid_spread(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Difference between weighted mid and top mid"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_mid_spread(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_mid_spread(self, data, depth_level: int = 10) -> np.float64 | None:
        """Difference between weighted mid and top mid"""
        depth_weighted_bid = self._depth_weighted_mid_price(data, depth_level=depth_level)
        mid_price = self._mid_price(data)

        if depth_weighted_bid is not None and mid_price is not None:
            return depth_weighted_bid - mid_price
        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="ask_slope",
        prototype="self.l2.ask_slope(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Ask curve steepness",
    )
    @property
    def ask_slope(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Ask curve steepness"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._ask_slope(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _ask_slope(self, data, depth_level: int = 10) -> np.float64 | None:
        """Ask curve steepness"""
        if data is None or len(data["ap"]) < depth_level:
            return None

        best_ask = data["ap"][0]
        slope_ask = data["ap"][depth_level - 1]

        return (slope_ask - best_ask) / best_ask

    @document(
        group=DOCUMENT_GROUP_L2,
        name="bid_slope",
        prototype="self.l2.bid_slope(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Bid curve steepness",
    )
    @property
    def bid_slope(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Bid curve steepness up to specified depth level"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._bid_slope(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _bid_slope(self, data, depth_level: int = 10) -> np.float64 | None:
        """Bid curve steepness up to specified depth level"""
        if data is None or len(data["bp"]) < depth_level:
            return None

        best_bid = data["bp"][0]
        slope_bid = data["bp"][depth_level - 1]

        return (best_bid - slope_bid) / best_bid

    @document(
        group=DOCUMENT_GROUP_L2,
        name="mean_ask_distance",
        prototype="self.l2.mean_ask_distance(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Average ask distance from mid",
    )
    @property
    def mean_ask_distance(self, depth_level: int = 10) -> np.ndarray[np.float64 | None] | None:
        """Average ask distance from mid"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._mean_ask_distance(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _mean_ask_distance(self, data, depth_level: int = 10) -> np.float64 | None:
        """Average ask distance from mid"""
        if data is None:
            return None

        mid_price = self._mid_price(data)
        if mid_price is None:
            return None

        distances = [price - mid_price for price in data["ap"][:depth_level]]
        return np.mean(distances) if distances else None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="mean_bid_distance",
        prototype="self.l2.mean_bid_distance(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Average bid distance from mid",
    )
    @property
    def mean_bid_distance(self, depth_level: int = 10) -> np.float64 | None:
        """Average bid distance from mid"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._mean_bid_distance(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _mean_bid_distance(self, data, depth_level: int = 10) -> np.float64 | None:
        """Average bid distance from mid"""
        if data is None:
            return None

        mid_price = self._mid_price(data)
        if mid_price is None:
            return None

        distances = [mid_price - price for price in data["bp"][:depth_level]]
        return np.mean(distances) if distances else None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="concentration_ratio",
        prototype="self.l2.concentration_ratio(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="% liquidity at level 1",
    )
    @property
    def concentration_ratio(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """% liquidity at level 1"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._concentration_ratio(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _concentration_ratio(self, data, depth_level: int = 10) -> np.float64 | None:
        """% liquidity at level 1"""
        if data is None:
            return None

        best_bid_size = data["bs"][0] if data["bs"] else None
        best_ask_size = data["as"][0] if data["as"] else None
        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0
        if total_bid_size + total_ask_size == 0:
            return None
        return (best_bid_size + best_ask_size) / (total_bid_size + total_ask_size)

    @document(
        group=DOCUMENT_GROUP_L2,
        name="bid_concentration",
        prototype="self.l2.bid_concentration(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="% bid liquidity at touch",
    )
    @property
    def bid_concentration(self, depth_level: int = 10) -> np.float64 | None:
        """% bid liquidity at touch"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._bid_concentration(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _bid_concentration(self, data, depth_level: int = 10) -> np.float64 | None:
        """% bid liquidity at touch"""
        if data is None:
            return None

        best_bid_size = data["bs"][0] if data["bs"] else None
        total_bid_size = sum(data["bs"][:depth_level]) if data["bs"] else 0
        if total_bid_size == 0:
            return None
        return best_bid_size / total_bid_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="ask_concentration",
        prototype="self.l2.ask_concentration(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="% ask liquidity at touch",
    )
    @property
    def ask_concentration(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """% ask liquidity at touch"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._ask_concentration(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _ask_concentration(self, data, depth_level: int = 10) -> np.float64 | None:
        """% ask liquidity at touch"""
        if data is None:
            return None

        best_ask_size = data["as"][0] if data["as"] else None
        total_ask_size = sum(data["as"][:depth_level]) if data["as"] else 0
        if total_ask_size == 0:
            return None
        return best_ask_size / total_ask_size

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_size_volatility",
        prototype="self.l2.depth_size_volatility(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Volatility of total depth volume",
    )
    @property
    def depth_size_volatility(self, depth_level: int = 10) -> npt.NDArray[np.float64 | None]:
        """Volatility of total depth volume"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_size_volatility(data.to_dict(), depth_level) for data in self._datas], dtype=np.float64)

    def _depth_size_volatility(self, data, depth_level: int = 10) -> np.float64 | None:
        """Volatility of total depth volume"""
        if data is None:
            return None

        sizes = data["bs"][:depth_level] + data["as"][:depth_level]
        return np.std(sizes) if sizes else None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="touch_liquidity_volatility",
        prototype="self.l2.touch_liquidity_volatility -> npt.NDArray[np.float64 | None]",
        docs="Volatility of size at L1",
    )
    @property
    def touch_liquidity_volatility(self) -> npt.NDArray[np.float64 | None]:
        """Volatility of size at L1"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._touch_liquidity_volatility(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _touch_liquidity_volatility(self, data) -> np.float64 | None:
        """Volatility of size at L1"""
        if data is None:
            return None

        best_bid_size = data["bs"][0] if data["bs"] else None
        best_ask_size = data["as"][0] if data["as"] else None

        if best_bid_size is not None and best_ask_size is not None:
            return np.std([best_bid_size, best_ask_size])

        return None

    @document(
        group=DOCUMENT_GROUP_L2,
        name="depth_size_volatility_volatility",
        prototype="self.l2.depth_size_volatility_volatility(depth_level: int = 10) -> npt.NDArray[np.float64 | None]",
        docs="Volatility of depth prices",
    )
    @property
    def depth_size_volatility_volatility(self) -> npt.NDArray[np.float64 | None]:
        """Volatility of depth prices"""
        if not self._datas:
            return np.empty(0, dtype=np.float64)
        return np.array([self._depth_price_volatility(data.to_dict()) for data in self._datas], dtype=np.float64)

    def _depth_price_volatility(self, data, depth_level: int = 10) -> np.float64 | None:
        """Volatility of depth prices"""
        if data is None:
            return None

        prices = data["bp"][:depth_level] + data["ap"][:depth_level]
        return np.std(prices) if prices else None


if __name__ == "__main__":
    l2 = TechnicalL2DataProvider(
        symbol="ACB", resolution="1H", from_time="2025-10-01", to_time="2025-12-31"
    )
    l2.load_data()
    print("Mid Price:", l2.mid_price)
