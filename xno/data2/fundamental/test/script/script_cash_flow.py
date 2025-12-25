from xno.data2.fundamental.entity import Period
from xno.data2.fundamental.provider import FundamentalDataProvider


def test_cash_flow():
    provider = FundamentalDataProvider.singleton()

    info = provider.get_cash_flow(symbol="VNM", period=Period.QUARTERLY, from_time=None, to_time=None)
    print(1, info)


if __name__ == "__main__":
    test_cash_flow()
