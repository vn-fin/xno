def test_get_basic_info():
    from xno.data2.fundamental.provider import FundamentalDataProvider

    provider = FundamentalDataProvider.singleton()
    
    info = provider.get_basic_info("ACB")
    print(1, info)
    
    info = provider.get_basic_info("ACB")
    print(2, info)

if __name__ == "__main__":
    test_get_basic_info()
