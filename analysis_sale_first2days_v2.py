"""
3つのセール初日2日間の比較データ収集（過去はCSV、現行はAPI）。
- GW2026: 2026-05-02 ~ 2026-05-03 (Shopify API + GA4 API)
- 新春2025: 2025-12-26 ~ 2025-12-27 (orders.csv + GA4 CSV)
- BF2025: 2025-11-15 ~ 2025-11-16 (orders.csv + GA4 CSV)

加えて 5月既存売上(API), 4月2026推移(API), 5月2025推移(CSV), 月次推移CSV を集約。
"""

import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
from datetime import datetime
from collections import defaultdict

from shopify_client import get_orders
from ga4_client import (
    get_traffic_overview, get_source_medium, get_device_breakdown, run_report,
)


def fetch_orders_df(start_date, end_date):
    print(f"  Shopify API: {start_date} ~ {end_date}")
    result = get_orders(start_date, end_date)
    if "errors" in result:
        return pd.DataFrame()
    edges = result["data"]["orders"]["edges"]
    rows = []
    for edge in edges:
        node = edge["node"]
        line_items = []
        for li in node["lineItems"]["edges"]:
            li = li["node"]
            line_items.append({
                "title": li["title"], "quantity": li["quantity"],
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


def load_orders_csv(start_date, end_date, csv_path="data/all/orders.csv"):
    """CSV から指定期間の注文を取得（PAID系のみ）"""
    print(f"  CSV: {start_date} ~ {end_date}")
    df = pd.read_csv(csv_path)
    df["Created at"] = pd.to_datetime(df["Created at"], errors="coerce", utc=True)
    df["created_jst"] = df["Created at"].dt.tz_convert("Asia/Tokyo")
    start = pd.to_datetime(start_date).tz_localize("Asia/Tokyo")
    end = pd.to_datetime(end_date).tz_localize("Asia/Tokyo") + pd.Timedelta(days=1)
    mask = (df["created_jst"] >= start) & (df["created_jst"] < end)
    df = df[mask].copy()
    # Shopify注文CSVは1注文が複数行に渡る（lineitemごと）
    # 注文ヘッダー行はFinancial Statusがある行
    header_rows = df[df["Financial Status"].notna() & (df["Financial Status"] != "")].copy()
    paid = header_rows[header_rows["Financial Status"].isin(
        ["paid", "partially_paid", "partially_refunded"]
    )]
    return df, paid  # 全行（line items）と支払い済みヘッダー


def csv_summarize(df_all, df_paid, label):
    """CSVから初日2日サマリ"""
    if df_paid.empty:
        return {"label": label, "order_count": 0, "total_sales": 0, "avg_order_value": 0,
                "total_discount": 0, "product_breakdown": {}, "discount_breakdown": {},
                "daily_sales": {}, "new_customer_ratio": None,
                "_note": "no orders"}
    n = len(df_paid)
    total = df_paid["Total"].astype(float).sum()
    aov = total / n
    discount = df_paid["Discount Amount"].fillna(0).astype(float).sum()
    daily = df_paid.copy()
    daily["date"] = daily["created_jst"].dt.date
    daily_g = daily.groupby("date").agg(orders=("Name", "nunique"), sales=("Total", "sum")).to_dict("index")

    # 商品別: df_all 全行から、注文Nameが df_paid に含まれるものを集計
    paid_names = set(df_paid["Name"])
    items = df_all[df_all["Name"].isin(paid_names) & df_all["Lineitem name"].notna()].copy()
    pb = defaultdict(lambda: {"quantity": 0, "sales": 0})
    for _, r in items.iterrows():
        name = r["Lineitem name"]
        qty = int(r["Lineitem quantity"]) if pd.notna(r["Lineitem quantity"]) else 0
        price = float(r["Lineitem price"]) if pd.notna(r["Lineitem price"]) else 0
        pb[name]["quantity"] += qty
        pb[name]["sales"] += price * qty

    db = {}
    for code, g in df_paid[df_paid["Discount Code"].notna() & (df_paid["Discount Code"] != "")].groupby("Discount Code"):
        db[code] = {"count": int(g["Name"].nunique()), "total": float(g["Total"].astype(float).sum())}

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
                         for k, v in daily_g.items()},
        "new_customer_ratio": None,  # CSVではnumberOfOrders情報なし
    }


def api_summarize(df, label):
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
        "label": label, "order_count": int(n), "total_sales": float(total),
        "avg_order_value": float(aov), "total_discount": float(discount),
        "product_breakdown": {k: {"quantity": int(v["quantity"]), "sales": float(v["sales"])}
                              for k, v in pb.items()},
        "discount_breakdown": db,
        "daily_sales": {str(k): {"orders": int(v["orders"]), "sales": float(v["sales"])}
                         for k, v in daily.items()},
        "new_customer_ratio": new_cust / n,
    }


def fetch_ga4(start_date, end_date):
    print(f"  GA4 API: {start_date} ~ {end_date}")
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
        m["sessions_funnel"] = sess
        m["add_to_carts"] = carts
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


def load_cvr_csv(path):
    """CVR breakdown CSVから日別 sessions/cart/checkout/completed を取得"""
    df = pd.read_csv(path)
    rows = []
    for _, r in df.iterrows():
        try:
            day = r["Day"]
            if pd.isna(day) or not day:
                continue
            rows.append({
                "date": str(day),
                "sessions": int(r["Sessions"]),
                "cart_adds": int(r["Sessions with cart additions"]),
                "checkouts": int(r["Sessions that reached checkout"]),
                "completed": int(r["Sessions that completed checkout"]),
                "cvr": float(r["Conversion rate"]) if pd.notna(r["Conversion rate"]) else 0,
            })
        except Exception:
            continue
    return rows


def main():
    out = {"generated_at": datetime.now().isoformat(), "periods": {}, "context": {}}

    # GW2026 (現行) - API
    print("\n=== GW2026 (5/2-5/3) ===")
    df = fetch_orders_df("2026-05-02", "2026-05-03")
    sales = api_summarize(df, "gw2026")
    ga4 = fetch_ga4("2026-05-02", "2026-05-03")
    ga4_m = summarize_ga4(ga4, "gw2026")
    if sales["order_count"] and ga4_m.get("total_sessions"):
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / ga4_m["total_sessions"]
    out["periods"]["gw2026"] = {"sales": sales, "ga4": ga4_m, "source": "shopify_api+ga4_api"}

    # 新春2025 (12/26-27) - CSV
    print("\n=== 新春2025 (12/26-27) ===")
    df_all, df_paid = load_orders_csv("2025-12-26", "2025-12-27")
    sales = csv_summarize(df_all, df_paid, "newyear2025")
    cvr_data = load_cvr_csv("data/newyear2025/cvr_breakdown.csv")
    cvr_2days = [r for r in cvr_data if r["date"] in ("2025-12-26", "2025-12-27")]
    sess_total = sum(r["sessions"] for r in cvr_2days)
    cart_total = sum(r["cart_adds"] for r in cvr_2days)
    chk_total = sum(r["checkouts"] for r in cvr_2days)
    comp_total = sum(r["completed"] for r in cvr_2days)
    ga4_m = {
        "label": "newyear2025",
        "total_sessions": sess_total,
        "add_to_carts": cart_total,
        "checkouts_ga4_csv": chk_total,
        "completed_ga4_csv": comp_total,
        "cart_rate": cart_total / sess_total if sess_total else 0,
        "shopify_cvr_csv": comp_total / sess_total if sess_total else 0,
        "daily_funnel": cvr_2days,
        "_source": "data/newyear2025/cvr_breakdown.csv",
    }
    if sales["order_count"] and sess_total:
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / sess_total
    out["periods"]["newyear2025"] = {"sales": sales, "ga4": ga4_m, "source": "orders.csv+cvr_breakdown.csv"}

    # BF2025 (11/15-16) - CSV
    print("\n=== BF2025 (11/15-16) ===")
    df_all, df_paid = load_orders_csv("2025-11-15", "2025-11-16")
    sales = csv_summarize(df_all, df_paid, "bf2025")
    cvr_data = load_cvr_csv("data/bf2025/cvr_breakdown.csv")
    cvr_2days = [r for r in cvr_data if r["date"] in ("2025-11-15", "2025-11-16")]
    sess_total = sum(r["sessions"] for r in cvr_2days)
    cart_total = sum(r["cart_adds"] for r in cvr_2days)
    chk_total = sum(r["checkouts"] for r in cvr_2days)
    comp_total = sum(r["completed"] for r in cvr_2days)
    ga4_m = {
        "label": "bf2025",
        "total_sessions": sess_total,
        "add_to_carts": cart_total,
        "checkouts_ga4_csv": chk_total,
        "completed_ga4_csv": comp_total,
        "cart_rate": cart_total / sess_total if sess_total else 0,
        "shopify_cvr_csv": comp_total / sess_total if sess_total else 0,
        "daily_funnel": cvr_2days,
        "_source": "data/bf2025/cvr_breakdown.csv",
    }
    if sales["order_count"] and sess_total:
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / sess_total
    out["periods"]["bf2025"] = {"sales": sales, "ga4": ga4_m, "source": "orders.csv+cvr_breakdown.csv"}

    # 5月2026 既存売上 (5/1-5/3) - API
    print("\n=== May2026 to date (5/1-5/3) ===")
    df = fetch_orders_df("2026-05-01", "2026-05-03")
    sales = api_summarize(df, "may2026_to_date")
    ga4 = fetch_ga4("2026-05-01", "2026-05-03")
    ga4_m = summarize_ga4(ga4, "may2026_to_date")
    if sales["order_count"] and ga4_m.get("total_sessions"):
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / ga4_m["total_sessions"]
    out["context"]["may2026_to_date"] = {"sales": sales, "ga4": ga4_m, "source": "shopify_api+ga4_api"}

    # 4月2026 - API
    print("\n=== Apr2026 (4/1-4/30) ===")
    df = fetch_orders_df("2026-04-01", "2026-04-30")
    sales = api_summarize(df, "apr2026")
    ga4 = fetch_ga4("2026-04-01", "2026-04-30")
    ga4_m = summarize_ga4(ga4, "apr2026")
    if sales["order_count"] and ga4_m.get("total_sessions"):
        sales["cvr_shopify_over_ga4"] = sales["order_count"] / ga4_m["total_sessions"]
    out["context"]["apr2026"] = {"sales": sales, "ga4": ga4_m, "source": "shopify_api+ga4_api"}

    # 5月2025 - CSV
    print("\n=== May2025 (5/1-5/31) ===")
    df_all, df_paid = load_orders_csv("2025-05-01", "2025-05-31")
    sales = csv_summarize(df_all, df_paid, "may2025")
    out["context"]["may2025"] = {"sales": sales, "source": "orders.csv"}

    # 月次推移 (sales_over_time.csv)
    print("\n=== 月次推移 ===")
    sot = pd.read_csv("data/all/sales_over_time.csv")
    monthly = []
    for _, r in sot.iterrows():
        try:
            month = str(r["Month"])
            if pd.isna(month) or "-" not in month:
                continue
            monthly.append({
                "month": month[:7],
                "orders": int(r["Orders"]) if pd.notna(r["Orders"]) else 0,
                "total_sales": float(r["Total sales"]) if pd.notna(r["Total sales"]) else 0,
                "discounts": float(r["Discounts"]) if pd.notna(r["Discounts"]) else 0,
            })
        except Exception:
            continue
    out["context"]["monthly_trend"] = monthly

    # 直近月次レポート（2026年）から1月,2月,3月の数字を補完
    print("\n=== 最近の月次（API） ===")
    for ym, (s, e) in {
        "2026-01": ("2026-01-01", "2026-01-31"),
        "2026-02": ("2026-02-01", "2026-02-28"),
        "2026-03": ("2026-03-01", "2026-03-31"),
    }.items():
        try:
            df = fetch_orders_df(s, e)
            sales = api_summarize(df, ym)
            out["context"].setdefault("recent_months_api", {})[ym] = {
                "order_count": sales["order_count"],
                "total_sales": sales["total_sales"],
                "total_discount": sales["total_discount"],
            }
        except Exception as e:
            print(f"  err {ym}: {e}")

    out_dir = os.path.join(os.path.dirname(__file__), "output", "campaign")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sale_first2days_data.json")
    with open(out_path, "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n保存完了: {out_path}")
    return out_path


if __name__ == "__main__":
    main()
