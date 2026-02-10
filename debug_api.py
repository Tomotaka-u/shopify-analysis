"""APIレスポンス確認用"""
import sys
import json
sys.path.insert(0, "/Users/uchida/shopify-analysis")
from shopify_client import get_orders
from datetime import datetime, timedelta

end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

print(f"取得期間: {start_date} 〜 {end_date}")
result = get_orders(start_date, end_date)
print(json.dumps(result, indent=2, ensure_ascii=False))
