from datetime import datetime
from xno.data2.fundamental.provider import FundamentalDataProvider


def test_get_price_volume():
    provider = FundamentalDataProvider.singleton()

    price_volume = provider.get_price_volume(
        symbol="ACB",
        from_time=datetime(2025, 1, 1),
        to_time=datetime(2025, 12, 31),
    )
    print(1, price_volume)

    price_volume = provider.get_price_volume(
        symbol="ACB",
        from_time=datetime(2025, 1, 1),
        to_time=datetime(2025, 12, 31),
    )
    print(2, price_volume)


if __name__ == "__main__":
    test_get_price_volume()
