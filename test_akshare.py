import sys
import traceback
from data_sources import akshare_source
import akshare as ak

def test_index(symbol):
    print(f"Testing index {symbol}...")
    try:
        df = ak.index_zh_a_hist(symbol=symbol, period="daily", start_date="20251027", end_date="20251031")
        print(f"Data type/shape: {type(df)} {getattr(df, 'shape', None)}")
    except Exception as e:
        print(f"Error for {symbol}: {type(e).__name__} - {e}")
        traceback.print_exc()

test_index("399150")
test_index("000001")
