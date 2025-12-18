import time
from xno.data2.fundamental.dataframe import FundamentalDataFrame

if __name__ == '__main__':
    fun = FundamentalDataFrame(symbol="ACB")

    for _ in range(10):
        time.sleep(5)
        pe = fun.pe_ratio()
        print("P/E Ratio:", pe)

        pb = fun.pb_ratio()
        print("P/B Ratio:", pb)

        ps = fun.ps_ratio()
        print("P/S Ratio:", ps)
