"""直近3ヶ月の月別売上推移分析"""

import sys
import json
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

sys.path.insert(0, "/Users/uchida/shopify-analysis")
from shopify_client import get_orders

# 直近3ヶ月を月単位で取得（APIリミット250件対策）
months = []
now = datetime.now()
for i in range(3):
    # 各月の初日と末日を計算
    if i == 0:
        month_end = now
        month_start = now.replace(day=1)
    else:
        prev = now.replace(day=1) - timedelta(days=1)  # 前月末日
        for j in range(1, i):
            prev = prev.replace(day=1) - timedelta(days=1)
        month_end = prev
        month_start = prev.replace(day=1)
    months.append((month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))

months.reverse()  # 古い順に

print("注文データを取得中...")
all_orders = []
for start, end in months:
    print(f"  {start} 〜 {end} ...", end=" ")
    result = get_orders(start, end)
    edges = result["data"]["orders"]["edges"]
    print(f"{len(edges)} 件")
    all_orders.extend(edges)

print(f"合計取得件数: {len(all_orders)} 件")

if len(all_orders) == 0:
    print("注文データが0件でした。")
    sys.exit(0)

# DataFrameに変換
records = []
for edge in all_orders:
    node = edge["node"]
    records.append({
        "order_id": node["name"],
        "created_at": pd.to_datetime(node["createdAt"]),
        "total_price": float(node["totalPriceSet"]["shopMoney"]["amount"]),
        "subtotal_price": float(node["subtotalPriceSet"]["shopMoney"]["amount"]),
        "total_discounts": float(node["totalDiscountsSet"]["shopMoney"]["amount"]),
        "currency": node["totalPriceSet"]["shopMoney"]["currencyCode"],
        "financial_status": node["displayFinancialStatus"],
        "fulfillment_status": node["displayFulfillmentStatus"],
        "customer_id": node["customer"]["id"] if node.get("customer") else None,
        "customer_orders_count": node["customer"]["numberOfOrders"] if node.get("customer") else None,
        "line_items": [li["node"]["title"] for li in node["lineItems"]["edges"]],
        "discount_codes": node.get("discountCodes", []),
    })

df = pd.DataFrame(records)
df = df.drop_duplicates(subset="order_id")  # 月またぎの重複除去
df["month"] = df["created_at"].dt.tz_localize(None).dt.to_period("M")

# === 月別集計 ===
monthly = df.groupby("month").agg(
    注文件数=("order_id", "count"),
    売上合計=("total_price", "sum"),
    平均注文額=("total_price", "mean"),
    割引合計=("total_discounts", "sum"),
).reset_index()
monthly["month_str"] = monthly["month"].astype(str)

print("\n" + "=" * 50)
print("月別売上サマリー")
print("=" * 50)
for _, row in monthly.iterrows():
    print(f"\n  {row['month_str']}:")
    print(f"    注文件数:   {row['注文件数']:>6.0f} 件")
    print(f"    売上合計:   ¥{row['売上合計']:>10,.0f}")
    print(f"    平均注文額: ¥{row['平均注文額']:>10,.0f}")
    print(f"    割引合計:   ¥{row['割引合計']:>10,.0f}")

# 前月比の計算
if len(monthly) >= 2:
    print("\n--- 前月比 ---")
    for i in range(1, len(monthly)):
        prev_sales = monthly.iloc[i - 1]["売上合計"]
        curr_sales = monthly.iloc[i]["売上合計"]
        change = ((curr_sales - prev_sales) / prev_sales) * 100 if prev_sales > 0 else 0
        print(f"  {monthly.iloc[i]['month_str']}: {change:+.1f}%")

# 全体サマリー
print(f"\n--- 3ヶ月全体 ---")
print(f"  総注文数: {len(df)} 件")
print(f"  総売上: ¥{df['total_price'].sum():,.0f}")
print(f"  平均注文額: ¥{df['total_price'].mean():,.0f}")

# === グラフ作成 ===
plt.rcParams["font.family"] = "Hiragino Sans"

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# 売上推移（棒グラフ）
bars1 = axes[0].bar(monthly["month_str"], monthly["売上合計"], color="#4A90D9", edgecolor="white")
axes[0].set_title("月別売上推移", fontsize=14, fontweight="bold")
axes[0].set_ylabel("売上 (円)")
for i, v in enumerate(monthly["売上合計"]):
    axes[0].text(i, v + v * 0.02, f"¥{v:,.0f}", ha="center", fontsize=10)
axes[0].set_ylim(0, monthly["売上合計"].max() * 1.15)

# 注文件数推移（棒グラフ）
bars2 = axes[1].bar(monthly["month_str"], monthly["注文件数"], color="#7BC67E", edgecolor="white")
axes[1].set_title("月別注文件数推移", fontsize=14, fontweight="bold")
axes[1].set_ylabel("注文件数")
for i, v in enumerate(monthly["注文件数"]):
    axes[1].text(i, v + v * 0.02, f"{v:.0f}件", ha="center", fontsize=10)
axes[1].set_ylim(0, monthly["注文件数"].max() * 1.15)

plt.tight_layout()
plt.savefig("/Users/uchida/shopify-analysis/output/monthly_sales.png", dpi=150, bbox_inches="tight")
print("\nグラフを output/monthly_sales.png に保存しました。")

# === 商品別販売数 ===
line_items_flat = []
for _, row in df.iterrows():
    for item in row["line_items"]:
        line_items_flat.append({"month": str(row["month"]), "product": item})

if line_items_flat:
    df_items = pd.DataFrame(line_items_flat)
    product_counts = df_items.groupby("product").size().sort_values(ascending=False)
    print("\n--- 商品別販売数（3ヶ月合計） ---")
    for product, count in product_counts.items():
        print(f"  {product}: {count} 件")

    # 月×商品のクロス集計
    cross = pd.crosstab(df_items["month"], df_items["product"])
    print("\n--- 月別×商品別 販売数 ---")
    print(cross.to_string())
