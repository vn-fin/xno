import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../..")))

from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

if __name__ == "__main__":
    from vnstock import Vnstock

    stock = Vnstock().stock(symbol='ACB', source='VCI')

    # Bảng cân đối kế toán - năm
    data = stock.finance.balance_sheet(period='year', lang='en', dropna=True)
    data.to_csv("data_vnstock_balance_sheet_year.csv")
    print("Bảng cân đối kế toán - năm")
    print(data)
    print("\n")

    # Bảng cân đối kế toán - quý
    data = stock.finance.balance_sheet(period='quarter', lang='en', dropna=True)
    data.to_csv("data_vnstock_balance_sheet_quarter.csv")
    print("Bảng cân đối kế toán - quý")
    print(data)
    print("\n")

    # Kết quả hoạt động kinh doanh
    data = stock.finance.income_statement(period='year', lang='en', dropna=True)
    data.to_csv("data_vnstock_income_statement_year.csv")
    print("Kết quả hoạt động kinh doanh")
    print(data)
    print("\n")

    # Lưu chuyển tiền tệ
    data = stock.finance.cash_flow(period='year', lang='en', dropna=True)
    data.to_csv("data_vnstock_cash_flow_year.csv")
    print("Lưu chuyển tiền tệ")
    print(data)
    print("\n")

    # Chỉ số tài chính
    data = stock.finance.ratio(period='year', lang='en', dropna=True)
    data.to_csv("data_vnstock_ratio_year.csv")
    print("Chỉ số tài chính")
    print(data)
    print("\n")
