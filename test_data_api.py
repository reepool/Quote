import sys
import os

try:
    import akshare as ak
    print("Akshare version:", ak.__version__)
    
    # Test BSE
    print("Testing BSE stock: 835185")
    try:
        df_bse = ak.stock_zh_a_hist(symbol="835185", period="daily", start_date="20230101", end_date="20230110", adjust="qfq")
        print("BSE shape:", df_bse.shape)
    except Exception as e:
        print("BSE test failed:", e)

    # Test HK
    print("Testing HK stock: 00700")
    try:
        df_hk = ak.stock_hk_hist(symbol="00700", period="daily", start_date="20230101", end_date="20230110", adjust="qfq")
        print("HK shape:", df_hk.shape)
    except Exception as e:
        print("HK test failed:", e)

    # Test US
    print("Testing US stock: AAPL")
    try:
        df_us = ak.stock_us_hist(symbol="105.AAPL", period="daily", start_date="20230101", end_date="20230110", adjust="qfq")
        print("US shape via 105.AAPL:", df_us.shape)
    except Exception as e:
        print("US test via 105.AAPL failed:", e)
        try:
            df_us2 = ak.stock_us_hist(symbol="AAPL", period="daily", start_date="20230101", end_date="20230110", adjust="qfq")
            print("US shape via AAPL:", df_us2.shape)
        except Exception as e2:
            print("US test via AAPL failed:", e2)

except ImportError:
    print("akshare is not installed")

try:
    import yfinance as yf
    print("yfinance version:", yf.__version__)
    print("Testing US stock via yfinance: AAPL")
    df_yf_us = yf.download("AAPL", start="2023-01-01", end="2023-01-10")
    print("US shape yfinance:", df_yf_us.shape)
except ImportError:
    print("yfinance is not installed")
except Exception as e:
    print("yfinance test failed:", e)

