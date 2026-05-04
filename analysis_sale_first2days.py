"""
3つのセールの初日2日間を比較するためのデータ収集スクリプト。
- GW2026: 2026-05-02 ~ 2026-05-03 (期間 5/2-5/10、新春2025と同じ割引)
- 新春2025: 2025-12-26 ~ 2025-12-27
- BF2025: 2025-11-15 ~ 2025-11-16

加えて、月次推移と5月既存売上(5/1-5/3)を取得する。
出力: output/campaign/sale_first2days_data.json
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
import pandas as pd
from datetime import datetime
from collections import defaultdict

from shopify_client import get_orders
from ga4_client import (
    get_traffic_overview,
    get_source_medium,
    get_device_breakdown,
    run_report,
)


PERIODS = {
    "gw2026": ("2026-05-02", "2026-05-03"),
    "newyear2025": ("2025-12-26", "2025-12-27"),
    "bf2025": ("2025-11-15", "2025-11-16"),
}

# 5月の既存売上推移（GW直前+セール開始2日）
MAY2026_RANGE = ("2026-05-01", "2026-05-03")

# 過去同月（参考: 5月の通常月パフォーマンス）
MAY2025_RANGE = ("2025-05-01", "2025-05-31")

# 直前1ヶ月（2026年4月）参考
APR2026_RANGE = ("2026-04-01", "2026-04-30")


def fetch_orders_df(start_date, end_date):
    print(f"  Shopify取得: {start_date} ~ {end_date}")
    result = get_orders(start_date, end_date)
    if "errors" in result:
        print(f"  ERR: {result['errors']}")
        return pd.DataFrame()
    edges = result["data"]["orders"]["edges"]
    rows = []
    for edge in edges:
        node = edge["node"]
        line_items = []
        for li in node["lineItems"]["edges"]:
            li = li["node"]
            line_items.append({
                "title": li["title"],
                "quantity": li["quantity"],
                "price": float(li["originalUnitPriceSet"]["shopMoney"]["amount"]),
            })
        codes = node.get("discountCodes", [])
        customer = node.get("customer", {}) or {}
        rows.append({
            "order_name": node["name"],
            "created_at": pd.to_datetime(node["createdAt"]),
            "total": float(node["totalPriceSet"]["shopMoney"]["amount"]),
            "discount_amount": float(node["totalDiscountsSet"]["shopMoney"]["amount"]),
            "discount_code": codes[0] if codes else None,
            "financial_status": node["displayFinancialStatus"],
            "line_items": line_items,
            "customer_orders": int(customer.get("numberOfOrders", "0") or 0),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["financial_status"].isin(["PAID", "PARTIALLY_PAID", "PARTIALLY_REFUNDED"])]
        df = df.drop_duplicates(subset=["order_name"])
    return df


def summarize_sales(df, label):
    if df.empty:
        return {"label": label, "order_count": 0, "total_sales": 0, "avg_order_value": 0,
                "total_discount": 0, "product_breakdown": {}, "discount_breakdown": {},
                "daily_sales": {}, "new_customer_ratio": 0}
    n = len(df)
    total = df["total"].sum()
    aov = total / n
    discount = df["discount_amount"].sum()
    pb = defaultdict(lambda: {"quantity": 0, "sales": 0})
    for _, r in df.iterrows():
        for it in r["line_items"]:
            pb[it["title"]]["quantity"] += it["quantity"]
            pb[it["title"]]["sales"] += it["price"] * it["quantity"]
    db = {}
    for code, g in df[df["discount_code"].notna()].groupby("discount_code"):
        db[code] = {"count": len(g), "total": float(g["total"].sum())}
    daily = df.groupby(df["created_at"].dt.date).agg(orders=("order_name", "count"),
                                                      sales=("total", "sum")).to_dict("index")
    new_cust = len(df[df["customer_orders"] <= 1])
    return {
        "label": label,
        "order_count": int(n),
        "total_sales": float(total),
        "avg_order_value": float(aov),
        "total_discount": float(discount),
        "product_breakdown": {k: {"quantity": int(v["quantity"]), "sales": float(v["sales"])}
                              for k, v in pb.items()},
        "discount_breakdown": db,
        "daily_sales": {str(k): {"orders": int(v["orders"]), "sales": float(v["sales"])}
                         for k, v in daily.items()},
        "new_customer_ratio": new_cust / n,
    }


def fetch_ga4(start_date, end_date):
    print(f"  GA4取得: {start_date} ~ {end_date}")
    out = {}
    try:
        out["traffic"] = get_traffic_overview(start_date, end_date)
        out["source"] = get_source_medium(start_date, end_date)
        out["device"] = get_device_breakdown(start_date, end_date)
        out["funnel"] = run_report(
            dimensions=["date"],
            metrics=["sessions", "addToCarts", "checkouts", "transactions", "purchaseRevenue"],
            start_date=start_date, end_date=end_date,
        )
    except Exception as e:
        print(f"  GA4 ERR: {e}")
    return out


def summarize_ga4(d, label):
    m = {"label": label}
    if not d:
        return m
    if "traffic" in d:
        df = d["traffic"]
        m["total_sessions"] = int(df["sessions"].sum())
        m["total_users"] = int(df["totalUsers"].sum())
        m["new_users"] = int(df["newUsers"].sum())
        m["channel_breakdown"] = {k: {"sessions": int(v["sessions"]), "totalUsers": int(v["totalUsers"])}
                                   for k, v in df.set_index("sessionDefaultChannelGroup")[["sessions", "totalUsers"]].to_dict("index").items()}
    if "funnel" in d:
        df = d["funnel"]
        sess = int(df["sessions"].sum())
        carts = int(df["addToCarts"].sum())
        chk = int(df["checkouts"].sum())
        tx = int(df["transactions"].sum())
        m["sessions_funnel"] = sess
        m["add_to_carts"] = carts
        m["checkouts_ga4"] = chk
        m["transactions_ga4"] = tx
        m["cart_rate"] = carts / sess if sess else 0
    if "device" in d:
        df = d["device"]
        m["device_breakdown"] = {k: {"sessions": int(v["sessions"]), "totalUsers": int(v["totalUsers"])}
                                  for k, v in df.set_index("deviceCategory")[["sessions", "totalUsers"]].to_dict("index").items()}
    if "source" in d:
        df = d["source"].sort_values("sessions", ascending=False).head(10)
        m["top_sources"] = []
        for _, r in df.iterrows():
            m["top_sources"].append({
                "source": r["sessionSource"], "medium": r["sessionMedium"],
                "sessions": int(r["sessions"]), "users": int(r["totalUsers"]),
            })
    return m


def fetch_period(label, start, end):
    print(f"\n--- {label}: {start} ~ {end} ---")
    df = fetch_orders_df(start, end)
    sales = summarize_sales(df, label)
    ga4 = fetch_ga4(start, end)
    ga4_metrics = summarize_ga4(ga4, label)
    if sales["order_count"] and ga4_metrics.get("total_sessions"):
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / ga4_metrics["total_sessions"]
    return {"sales": sales, "ga4": ga4_metrics}


def fetch_monthly_summary(label, start, end):
    print(f"\n--- {label} 月次サマリ: {start} ~ {end} ---")
    df = fetch_orders_df(start, end)
    sales = summarize_sales(df, label)
    ga4 = fetch_ga4(start, end)
    ga4_m = summarize_ga4(ga4, label)
    if sales["order_count"] and ga4_m.get("total_sessions"):
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / ga4_m["total_sessions"]
    return {"sales": sales, "ga4": ga4_m}


def main():
    out = {"generated_at": datetime.now().isoformat(), "periods": {}, "context": {}}

    for k, (s, e) in PERIODS.items():
        out["periods"][k] = fetch_period(k, s, e)

    # コンテキスト: 5月既存(5/1-5/3) / 4月通常 / 前年5月
    out["context"]["may2026_to_date"] = fetch_monthly_summary("may2026_to_date", *MAY2026_RANGE)
    out["context"]["apr2026"] = fetch_monthly_summary("apr2026", *APR2026_RANGE)
    out["context"]["may2025"] = fetch_monthly_summary("may2025", *MAY2025_RANGE)

    out_dir = os.path.join(os.path.dirname(__file__), "output", "campaign")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sale_first2days_data.json")
    with open(out_path, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n保存完了: {out_path}")


if __name__ == "__main__":
    main()
