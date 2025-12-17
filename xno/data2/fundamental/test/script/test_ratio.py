import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../..")))

from dotenv import load_dotenv
load_dotenv("../xalpha/.env")

from xno.data2.fundamental import FundamentalDataProvider

fundamental_data_provider = FundamentalDataProvider.singleton()

def test_provider_get_ratio():
    ratio = fundamental_data_provider.get_ratio(symbol="ACB", period="year")
    print(ratio)
    ratio.to_csv("data_fundamental_ratio.csv")

if __name__ == "__main__":
    test_provider_get_ratio()