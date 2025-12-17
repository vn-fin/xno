import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../..")))

from dotenv import load_dotenv
load_dotenv("../xalpha/.env")

from xno.data2.fundamental import FundamentalDataProvider

fundamental_data_provider = FundamentalDataProvider.singleton()

def test_provider_get_balance_sheet():
    balance_sheet = fundamental_data_provider.get_balance_sheet(symbol="ACB", period="year")
    print(balance_sheet)
    balance_sheet.to_csv("data_fundamental_balance_sheet.csv")

if __name__ == "__main__":
    test_provider_get_balance_sheet()