"""
購入者属性分析スクリプト
Shopify顧客データ + GA4デモグラフィクスデータを統合して
性別・年齢・国籍・居住地の分析ダッシュボードを生成
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
import pandas as pd
from datetime import datetime
from collections import defaultdict, Counter
from jinja2 import Environment, FileSystemLoader

from shopify_client import get_orders, shopify_graphql
from ga4_client import run_report


# === 設定 ===
# 全期間のデータを取得（ストア開設から現在まで）
DATA_START_DATE = "2024-01-01"
DATA_END_DATE = datetime.now().strftime("%Y-%m-%d")
OUTPUT_PATH = "output/buyer_demographics.html"


def fetch_shopify_customers():
    """Shopify顧客データを取得（住所情報含む）"""
    print("[1/3] Shopify顧客データ取得中...")

    # カスタムクエリで顧客データ取得（2025-01 API対応のフィールド名）
    query = """
    query($first: Int!, $after: String) {
        customers(first: $first, after: $after) {
            edges {
                cursor
                node {
                    id
                    email
                    firstName
                    lastName
                    numberOfOrders
                    amountSpent { amount currencyCode }
                    createdAt
                    updatedAt
                    tags
                    defaultAddress {
                        country
                        countryCodeV2
                        province
                        city
                    }
                }
            }
            pageInfo { hasNextPage }
        }
    }
    """

    all_edges = []
    cursor = None

    while True:
        variables = {"first": 250, "after": cursor}
        result = shopify_graphql(query, variables)

        if "errors" in result:
            print(f"  エラー: {result['errors']}")
            break

        edges = result.get("data", {}).get("customers", {}).get("edges", [])
        all_edges.extend(edges)

        if not result["data"]["customers"]["pageInfo"]["hasNextPage"]:
            break
        cursor = edges[-1]["cursor"]

    print(f"  顧客数: {len(all_edges)}件")

    customers = []
    for edge in all_edges:
        node = edge["node"]
        addr = node.get("defaultAddress") or {}
        customers.append({
            "id": node["id"],
            "email": node.get("email", ""),
            "first_name": node.get("firstName", ""),
            "last_name": node.get("lastName", ""),
            "orders_count": node.get("numberOfOrders", "0"),
            "total_spent": float((node.get("amountSpent") or {}).get("amount", 0)),
            "currency": (node.get("amountSpent") or {}).get("currencyCode", "JPY"),
            "country": addr.get("country", ""),
            "country_code": addr.get("countryCodeV2", ""),
            "province": addr.get("province", ""),
            "city": addr.get("city", ""),
            "tags": node.get("tags", []),
            "created_at": node.get("createdAt", ""),
        })

    return customers


def fetch_shopify_orders():
    """Shopify注文データから顧客住所を取得"""
    print("[2/3] Shopify注文データ取得中...")

    # 注文データから顧客住所を取得するカスタムクエリ
    query = """
    query($query: String!, $first: Int!, $after: String) {
        orders(query: $query, first: $first, after: $after) {
            edges {
                cursor
                node {
                    id
                    name
                    createdAt
                    totalPriceSet { shopMoney { amount currencyCode } }
                    displayFinancialStatus
                    customer {
                        id
                        email
                    }
                    shippingAddress {
                        country
                        countryCodeV2
                        province
                        city
                    }
                    billingAddress {
                        country
                        countryCodeV2
                        province
                        city
                    }
                }
            }
            pageInfo { hasNextPage }
        }
    }
    """

    all_edges = []
    cursor = None

    while True:
        variables = {
            "query": f"created_at:>={DATA_START_DATE} created_at:<={DATA_END_DATE}",
            "first": 250,
            "after": cursor,
        }
        result = shopify_graphql(query, variables)

        if "errors" in result:
            print(f"  エラー: {result['errors']}")
            break

        edges = result["data"]["orders"]["edges"]
        all_edges.extend(edges)

        if not result["data"]["orders"]["pageInfo"]["hasNextPage"]:
            break

        cursor = edges[-1]["cursor"]

    print(f"  注文数: {len(all_edges)}件")

    orders = []
    for edge in all_edges:
        node = edge["node"]
        financial_status = node.get("displayFinancialStatus", "")

        # PAID/PARTIALLY_PAID のみ
        if financial_status not in ("PAID", "PARTIALLY_PAID", "PARTIALLY_REFUNDED"):
            continue

        # 住所は shippingAddress > billingAddress の優先順位
        addr = node.get("shippingAddress") or node.get("billingAddress") or {}

        orders.append({
            "order_id": node["id"],
            "order_name": node["name"],
            "created_at": node["createdAt"],
            "total": float(node["totalPriceSet"]["shopMoney"]["amount"]),
            "currency": node["totalPriceSet"]["shopMoney"]["currencyCode"],
            "customer_id": (node.get("customer") or {}).get("id", ""),
            "customer_email": (node.get("customer") or {}).get("email", ""),
            "country": addr.get("country", ""),
            "country_code": addr.get("countryCodeV2", ""),
            "province": addr.get("province", ""),
            "city": addr.get("city", ""),
        })

    print(f"  有効注文数: {len(orders)}件")
    return orders


def fetch_ga4_demographics():
    """GA4からデモグラフィクスデータを取得"""
    print("[3/3] GA4デモグラフィクスデータ取得中...")

    data = {}

    try:
        # 性別 × 購入データ
        print("  性別データ取得中...")
        gender_df = run_report(
            dimensions=["userGender"],
            metrics=["transactions", "totalUsers", "purchaseRevenue"],
            start_date=DATA_START_DATE,
            end_date="today",
        )
        data["gender"] = gender_df
        print(f"    {len(gender_df)}行取得")

        # 年齢層 × 購入データ
        print("  年齢層データ取得中...")
        age_df = run_report(
            dimensions=["userAgeBracket"],
            metrics=["transactions", "totalUsers", "purchaseRevenue"],
            start_date=DATA_START_DATE,
            end_date="today",
        )
        data["age"] = age_df
        print(f"    {len(age_df)}行取得")

        # 性別 × 年齢 クロスデータ
        print("  性別×年齢クロスデータ取得中...")
        cross_df = run_report(
            dimensions=["userGender", "userAgeBracket"],
            metrics=["transactions", "totalUsers", "purchaseRevenue"],
            start_date=DATA_START_DATE,
            end_date="today",
        )
        data["gender_age_cross"] = cross_df
        print(f"    {len(cross_df)}行取得")

        # 国別（GA4）
        print("  国別データ取得中...")
        country_df = run_report(
            dimensions=["country"],
            metrics=["transactions", "totalUsers", "purchaseRevenue"],
            start_date=DATA_START_DATE,
            end_date="today",
        )
        data["country"] = country_df
        print(f"    {len(country_df)}行取得")

        # 都市別（GA4）
        print("  都市別データ取得中...")
        city_df = run_report(
            dimensions=["city"],
            metrics=["transactions", "totalUsers", "purchaseRevenue"],
            start_date=DATA_START_DATE,
            end_date="today",
        )
        data["city"] = city_df
        print(f"    {len(city_df)}行取得")

        print("  GA4データ取得完了 ✅")
    except Exception as e:
        print(f"  GA4エラー: {e}")

    return data


def analyze_customer_segments(customers):
    """顧客セグメント分析（F頻度・M金額）"""
    # 1. 購入回数分布
    freq_dist = {"1回": 0, "2回": 0, "3回": 0, "4回": 0, "5回以上": 0}
    for c in customers:
        count = int(c["orders_count"])
        if count == 1:
            freq_dist["1回"] += 1
        elif count == 2:
            freq_dist["2回"] += 1
        elif count == 3:
            freq_dist["3回"] += 1
        elif count == 4:
            freq_dist["4回"] += 1
        elif count >= 5:
            freq_dist["5回以上"] += 1

    # 2. LTV分布（累計購入金額）
    # バケット: ~3k, ~5k, ~10k, ~30k, 30k+
    ltv_dist = {
        "〜¥3,000": 0,
        "〜¥5,000": 0,
        "〜¥10,000": 0,
        "〜¥30,000": 0,
        "¥30,000+": 0,
    }
    
    for c in customers:
        spent = c["total_spent"]
        if spent <= 3000:
            ltv_dist["〜¥3,000"] += 1
        elif spent <= 5000:
            ltv_dist["〜¥5,000"] += 1
        elif spent <= 10000:
            ltv_dist["〜¥10,000"] += 1
        elif spent <= 30000:
            ltv_dist["〜¥30,000"] += 1
        else:
            ltv_dist["¥30,000+"] += 1

    return {
        "frequency": freq_dist,
        "ltv": ltv_dist,
        "total_repeaters": sum(v for k, v in freq_dist.items() if k != "1回"),
        "repeat_rate": sum(v for k, v in freq_dist.items() if k != "1回") / len(customers) if customers else 0
    }


def analyze_shopify_geo(orders, customers):
    """Shopifyデータから地理情報と「質」を集計"""
    
    # helper: 顧客IDから顧客情報を引けるようにする
    cust_map = {c["id"]: c for c in customers}

    # 集計用辞書: {Region: {orders, revenue, customers(set), repeaters(set)}}
    country_stats = defaultdict(lambda: {"orders": 0, "revenue": 0, "customers": set()})
    province_stats = defaultdict(lambda: {"orders": 0, "revenue": 0, "customers": set()})
    city_stats = defaultdict(lambda: {"orders": 0, "revenue": 0, "customers": set(), "country": ""})

    # 注文ベースで集計（売上、回数、ユニーク顧客）
    for order in orders:
        cid = order["customer_id"]
        country = order["country"] or "(不明)"
        province = order["province"]
        city = order["city"] or "(不明)"
        rev = order["total"]

        # 国
        country_stats[country]["orders"] += 1
        country_stats[country]["revenue"] += rev
        if cid: country_stats[country]["customers"].add(cid)

        # 都道府県（日本のみ）
        if order["country_code"] == "JP" and province:
            province_stats[province]["orders"] += 1
            province_stats[province]["revenue"] += rev
            if cid: province_stats[province]["customers"].add(cid)

        # 都市
        city_stats[city]["orders"] += 1
        city_stats[city]["revenue"] += rev
        city_stats[city]["country"] = country
        if cid: city_stats[city]["customers"].add(cid)

    # 統計計算（LTV, リピート率）のヘルパー
    def calc_stats(raw_stats, limit=15):
        results = []
        for name, data in raw_stats.items():
            n_cust = len(data["customers"])
            n_orders = data["orders"]
            revenue = data["revenue"]
            
            # リピーター数（この地域で購入し、かつ通算2回以上購入している人）
            # ※本来は「この地域から2回以上」だが、簡易的に「顧客属性のリピーターフラグ」で判定
            n_repeaters = 0
            for cid in data["customers"]:
                c = cust_map.get(cid)
                if c and int(c.get("orders_count", 0)) >= 2:
                    n_repeaters += 1
            
            results.append({
                "name": name,
                "orders": n_orders,
                "revenue": revenue,
                "customers": n_cust,
                "ltv": revenue / n_cust if n_cust > 0 else 0,
                "repeat_rate": n_repeaters / n_cust if n_cust > 0 else 0,
                "country": data.get("country", "")
            })
        
        # 注文数順にソート
        return sorted(results, key=lambda x: x["orders"], reverse=True)[:limit]

    return {
        "top_countries": calc_stats(country_stats),
        "top_provinces": calc_stats(province_stats),
        "top_cities": calc_stats(city_stats),
        "total_countries": len(country_stats),
    }


def analyze_ga4_demographics(ga4_data):
    """GA4デモグラフィクスデータを分析用に整形"""
    result = {
        "has_gender": False,
        "has_age": False,
        "gender_data": [],
        "age_data": [],
        "cross_data": [],
        "ga4_countries": [],
        "ga4_cities": [],
        "gender_mode": "transactions",  # transactions or users
        "age_mode": "transactions",
    }

    # 性別データ
    if "gender" in ga4_data and not ga4_data["gender"].empty:
        df = ga4_data["gender"]
        # (not set) と unknown を除外
        valid = df[~df["userGender"].isin(["(not set)", "unknown"])]
        
        # 有効データが存在し、かつユーザー数が1以上ある場合
        if not valid.empty and valid["totalUsers"].sum() > 0:
            result["has_gender"] = True
            total_transactions = valid["transactions"].sum()
            total_users = valid["totalUsers"].sum()
            total_revenue = valid["purchaseRevenue"].sum()

            # トランザクションがあれば購入者ベース、なければユーザーベース
            if total_transactions > 0:
                result["gender_mode"] = "transactions"
                base_total = total_transactions
            else:
                result["gender_mode"] = "users"
                base_total = total_users

            for _, row in valid.iterrows():
                label = {"male": "男性", "female": "女性"}.get(
                    row["userGender"], row["userGender"]
                )
                
                # モードに応じた構成比
                if result["gender_mode"] == "transactions":
                    main_pct = row["transactions"] / base_total if base_total else 0
                else:
                    main_pct = row["totalUsers"] / base_total if base_total else 0

                result["gender_data"].append({
                    "label": label,
                    "raw": row["userGender"],
                    "transactions": int(row["transactions"]),
                    "users": int(row["totalUsers"]),
                    "revenue": float(row["purchaseRevenue"]),
                    "tx_pct": row["transactions"] / total_transactions if total_transactions else 0,
                    "user_pct": row["totalUsers"] / total_users if total_users else 0,
                    "rev_pct": row["purchaseRevenue"] / total_revenue if total_revenue else 0,
                    "main_pct": main_pct,
                })

    # 年齢層データ
    if "age" in ga4_data and not ga4_data["age"].empty:
        df = ga4_data["age"]
        valid = df[~df["userAgeBracket"].isin(["(not set)", "unknown"])]
        
        if not valid.empty and valid["totalUsers"].sum() > 0:
            result["has_age"] = True
            total_transactions = valid["transactions"].sum()
            total_users = valid["totalUsers"].sum()
            total_revenue = valid["purchaseRevenue"].sum()
            
            if total_transactions > 0:
                result["age_mode"] = "transactions"
                base_total = total_transactions
            else:
                result["age_mode"] = "users"
                base_total = total_users

            # 年齢順にソート
            age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
            valid = valid.copy()
            valid["sort_key"] = valid["userAgeBracket"].apply(
                lambda x: age_order.index(x) if x in age_order else 99
            )
            valid = valid.sort_values("sort_key")

            for _, row in valid.iterrows():
                if result["age_mode"] == "transactions":
                    main_pct = row["transactions"] / base_total if base_total else 0
                else:
                    main_pct = row["totalUsers"] / base_total if base_total else 0

                result["age_data"].append({
                    "label": row["userAgeBracket"],
                    "transactions": int(row["transactions"]),
                    "users": int(row["totalUsers"]),
                    "revenue": float(row["purchaseRevenue"]),
                    "tx_pct": row["transactions"] / total_transactions if total_transactions else 0,
                    "user_pct": row["totalUsers"] / total_users if total_users else 0,
                    "rev_pct": row["purchaseRevenue"] / total_revenue if total_revenue else 0,
                    "main_pct": main_pct,
                })

    # 性別×年齢クロス
    if "gender_age_cross" in ga4_data and not ga4_data["gender_age_cross"].empty:
        df = ga4_data["gender_age_cross"]
        valid = df[
            (df["userGender"] != "(not set)") &
            (df["userAgeBracket"] != "(not set)")
        ]
        if not valid.empty:
            for _, row in valid.iterrows():
                gender_label = {"male": "男性", "female": "女性"}.get(
                    row["userGender"], row["userGender"]
                )
                result["cross_data"].append({
                    "gender": gender_label,
                    "age": row["userAgeBracket"],
                    "transactions": int(row["transactions"]),
                    "users": int(row["totalUsers"]),
                    "revenue": float(row["purchaseRevenue"]),
                })

    # GA4国別
    if "country" in ga4_data and not ga4_data["country"].empty:
        df = ga4_data["country"]
        df = df[df["country"] != "(not set)"]
        df = df.sort_values("transactions", ascending=False).head(15)
        for _, row in df.iterrows():
            result["ga4_countries"].append({
                "name": row["country"],
                "transactions": int(row["transactions"]),
                "users": int(row["totalUsers"]),
                "revenue": float(row["purchaseRevenue"]),
            })

    # GA4都市別
    if "city" in ga4_data and not ga4_data["city"].empty:
        df = ga4_data["city"]
        df = df[df["city"] != "(not set)"]
        df = df.sort_values("transactions", ascending=False).head(15)
        for _, row in df.iterrows():
            result["ga4_cities"].append({
                "name": row["city"],
                "transactions": int(row["transactions"]),
                "users": int(row["totalUsers"]),
                "revenue": float(row["purchaseRevenue"]),
            })

    return result


def render_html(shopify_geo, ga4_demo, segments, orders, customers):
    """Jinja2テンプレートでHTMLレンダリング"""
    env = Environment(
        loader=FileSystemLoader("report/templates"),
        autoescape=False,
    )

    # カスタムフィルター
    env.filters["fmt_yen"] = lambda x: f"{x:,.0f}"
    env.filters["fmt_num"] = lambda x: f"{int(x):,}"
    env.filters["fmt_pct"] = lambda x: f"{x:.1%}"

    template = env.get_template("buyer_demographics.html")

    # KPIデータ
    total_orders = len(orders)
    total_customers = len(customers)
    total_revenue = sum(o["total"] for o in orders)
    
    # テンプレート用ヘルパー
    def prepare_bar_data(items, key="orders"):
        if not items: return []
        max_val = max(item[key] for item in items)
        for item in items:
            item["pct"] = item[key] / total_orders if total_orders else 0
            item["width"] = item[key] / max_val * 100 if max_val else 0
        return items

    context = {
        "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "data_period": f"{DATA_START_DATE} 〜 {DATA_END_DATE}",
        "total_orders": total_orders,
        "total_customers": total_customers,
        "total_revenue": total_revenue,
        "total_countries": shopify_geo["total_countries"],
        "avg_order_value": total_revenue / total_orders if total_orders else 0,
        "repeat_rate": segments["repeat_rate"],

        # Segments
        "freq_dist": segments["frequency"],
        "ltv_dist": segments["ltv"],

        # GA4 demographics
        "has_gender": ga4_demo["has_gender"],
        "has_age": ga4_demo["has_age"],
        "gender_data": ga4_demo["gender_data"],
        "age_data": ga4_demo["age_data"],
        "cross_data": ga4_demo["cross_data"],
        "ga4_demo": ga4_demo,  # Pass full object for modes
        # Shopify geo (Enhanced)
        "top_countries": prepare_bar_data(shopify_geo["top_countries"]),
        "top_provinces": prepare_bar_data(shopify_geo["top_provinces"]),
        "top_cities": prepare_bar_data(shopify_geo["top_cities"]),

        # GA4 geo
        "ga4_countries": ga4_demo["ga4_countries"],
        "ga4_cities": ga4_demo["ga4_cities"],
    }

    return template.render(**context)


def main():
    print("=" * 50)
    print("購入者属性分析ダッシュボード")
    print(f"対象期間: {DATA_START_DATE} 〜 {DATA_END_DATE}")
    print("=" * 50)
    print()

    # 1. Shopify顧客データ
    customers = fetch_shopify_customers()

    # 2. Shopify注文データ（住所付き）
    orders = fetch_shopify_orders()

    # 3. GA4デモグラフィクス
    ga4_data = fetch_ga4_demographics()

    print()
    print("集計中...")

    # 顧客セグメント分析
    segments = analyze_customer_segments(customers)

    # Shopify地理情報の集計（質分析含む）
    shopify_geo = analyze_shopify_geo(orders, customers)

    # GA4デモグラフィクスの整形
    ga4_demo = analyze_ga4_demographics(ga4_data)

    # HTML生成
    print("HTMLレンダリング中...")
    html = render_html(shopify_geo, ga4_demo, segments, orders, customers)

    os.makedirs("output", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ ダッシュボード生成完了: {OUTPUT_PATH}")

    # サマリー出力
    print()
    print("=" * 50)
    print("サマリー")
    print("=" * 50)
    print(f"  総顧客数: {len(customers):,}人")
    print(f"  リピート率: {segments['repeat_rate']:.1%}")
    print(f"  対象国数: {shopify_geo['total_countries']}カ国")



if __name__ == "__main__":
    main()
