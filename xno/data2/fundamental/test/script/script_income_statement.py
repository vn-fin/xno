from xno.data2.fundamental.entity import Period
from xno.data2.fundamental.provider import FundamentalDataProvider


def test_income_statement():
    provider = FundamentalDataProvider.singleton()

    info = provider.get_income_statement(symbol="VNM", period=Period.QUARTERLY, from_time=None, to_time=None)
    print(1, info)


if __name__ == "__main__":
    test_income_statement()
