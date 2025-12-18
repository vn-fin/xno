from xno.data2.fundamental import FundamentalDataProvider

fundamental_data_provider = FundamentalDataProvider.singleton()


def test_provider_get_ratio():
    ratio = fundamental_data_provider.get_ratio(symbol="ACB", period="year")
    print(ratio)
    ratio.to_csv("data_fundamental_ratio.csv")


if __name__ == "__main__":
    test_provider_get_ratio()
