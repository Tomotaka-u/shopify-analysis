"""
週次比較分析スクリプト
今週 vs 先週の売上・トラフィック比較レポートを生成
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

from shopify_client import get_orders
from ga4_client import (
    get_traffic_overview,
    get_daily_traffic,
    get_source_medium,
    get_device_breakdown,
    run_report,
)

# === 期間設定 ===
THIS_WEEK_START = "2026-03-08"
THIS_WEEK_END = "2026-03-14"
LAST_WEEK_START = "2026-03-01"
LAST_WEEK_END = "2026-03-07"


def fetch_orders(start_date, end_date):
    """Shopify APIから注文データを取得してDataFrameに変換"""
    print(f"  Shopify注文取得中: {start_date} ~ {end_date}")
    result = get_orders(start_date, end_date)

    if "errors" in result:
        print(f"  エラー: {result['errors']}")
        return pd.DataFrame()

    edges = result["data"]["orders"]["edges"]
    print(f"  取得件数: {len(edges)}件")

    rows = []
    for edge in edges:
        node = edge["node"]
        total = float(node["totalPriceSet"]["shopMoney"]["amount"])
        subtotal = float(node["subtotalPriceSet"]["shopMoney"]["amount"])
        discount = float(node["totalDiscountsSet"]["shopMoney"]["amount"])

        # 商品情報
        line_items = []
        for li_edge in node["lineItems"]["edges"]:
            li = li_edge["node"]
            line_items.append({
                "title": li["title"],
                "quantity": li["quantity"],
                "price": float(li["originalUnitPriceSet"]["shopMoney"]["amount"]),
            })

        # ディスカウントコード
        discount_codes = node.get("discountCodes", [])
        discount_code = discount_codes[0] if discount_codes else None

        # 顧客情報
        customer = node.get("customer", {})
        customer_orders = customer.get("numberOfOrders", "0") if customer else "0"

        rows.append({
            "order_name": node["name"],
            "created_at": pd.to_datetime(node["createdAt"]),
            "total": total,
            "subtotal": subtotal,
            "discount_amount": discount,
            "discount_code": discount_code,
            "financial_status": node["displayFinancialStatus"],
            "line_items": line_items,
            "customer_orders": int(customer_orders) if customer_orders else 0,
            "referrer_url": node.get("referrerUrl", ""),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        # PAID/PARTIALLY_PAID のみ（返金済み除外）
        df = df[df["financial_status"].isin(["PAID", "PARTIALLY_PAID", "PARTIALLY_REFUNDED"])]
        # 重複排除
        df = df.drop_duplicates(subset=["order_name"])
    return df


def calc_sales_metrics(df, label):
    """売上KPIを計算"""
    if df.empty:
        return {
            "label": label,
            "order_count": 0,
            "total_sales": 0,
            "avg_order_value": 0,
            "total_discount": 0,
            "product_breakdown": {},
            "discount_breakdown": {},
            "daily_sales": {},
            "new_customer_ratio": 0,
        }

    order_count = len(df)
    total_sales = df["total"].sum()
    avg_order_value = total_sales / order_count if order_count > 0 else 0
    total_discount = df["discount_amount"].sum()

    # 商品別売上
    product_sales = defaultdict(lambda: {"quantity": 0, "sales": 0})
    for _, row in df.iterrows():
        for item in row["line_items"]:
            product_sales[item["title"]]["quantity"] += item["quantity"]
            product_sales[item["title"]]["sales"] += item["price"] * item["quantity"]

    # ディスカウントコード別
    discount_breakdown = {}
    discount_df = df[df["discount_code"].notna()]
    if not discount_df.empty:
        for code, group in discount_df.groupby("discount_code"):
            discount_breakdown[code] = {
                "count": len(group),
                "total": group["total"].sum(),
            }

    # 日別売上
    daily = df.groupby(df["created_at"].dt.date).agg(
        orders=("order_name", "count"),
        sales=("total", "sum"),
    ).to_dict("index")

    # 新規顧客率（注文回数1回 = 新規）
    new_customers = len(df[df["customer_orders"] <= 1])
    new_customer_ratio = new_customers / order_count if order_count > 0 else 0

    return {
        "label": label,
        "order_count": order_count,
        "total_sales": total_sales,
        "avg_order_value": avg_order_value,
        "total_discount": total_discount,
        "product_breakdown": dict(product_sales),
        "discount_breakdown": discount_breakdown,
        "daily_sales": {str(k): v for k, v in daily.items()},
        "new_customer_ratio": new_customer_ratio,
    }


def fetch_ga4_data(start_date, end_date):
    """GA4からトラフィックデータを取得"""
    print(f"  GA4データ取得中: {start_date} ~ {end_date}")

    data = {}

    try:
        # トラフィック概要
        traffic = get_traffic_overview(start_date, end_date)
        data["traffic_overview"] = traffic

        # 日別トラフィック
        daily = get_daily_traffic(start_date, end_date)
        data["daily_traffic"] = daily

        # 参照元/メディア
        source_medium = get_source_medium(start_date, end_date)
        data["source_medium"] = source_medium

        # デバイス
        device = get_device_breakdown(start_date, end_date)
        data["device"] = device

        # CVRファネル
        funnel = run_report(
            dimensions=["date"],
            metrics=["sessions", "addToCarts", "checkouts", "transactions", "purchaseRevenue"],
            start_date=start_date,
            end_date=end_date,
        )
        data["funnel"] = funnel

        print(f"  GA4取得完了")
    except Exception as e:
        print(f"  GA4エラー: {e}")

    return data


def calc_ga4_metrics(ga4_data, label):
    """GA4データからKPIを計算"""
    metrics = {"label": label}

    if not ga4_data:
        return metrics

    # トラフィック概要
    if "traffic_overview" in ga4_data:
        df = ga4_data["traffic_overview"]
        metrics["total_sessions"] = int(df["sessions"].sum())
        metrics["total_users"] = int(df["totalUsers"].sum())
        metrics["new_users"] = int(df["newUsers"].sum())
        metrics["avg_bounce_rate"] = df["bounceRate"].mean()
        metrics["channel_breakdown"] = df.set_index("sessionDefaultChannelGroup")[
            ["sessions", "totalUsers"]
        ].to_dict("index")

    # ファネル
    if "funnel" in ga4_data:
        df = ga4_data["funnel"]
        metrics["total_add_to_carts"] = int(df["addToCarts"].sum())
        metrics["total_checkouts"] = int(df["checkouts"].sum())
        metrics["total_transactions"] = int(df["transactions"].sum())
        metrics["total_revenue_ga4"] = df["purchaseRevenue"].sum()
        sessions = df["sessions"].sum()
        if sessions > 0:
            metrics["cvr"] = df["transactions"].sum() / sessions
            metrics["cart_rate"] = df["addToCarts"].sum() / sessions
            metrics["checkout_rate"] = df["checkouts"].sum() / sessions

    # デバイス
    if "device" in ga4_data:
        df = ga4_data["device"]
        metrics["device_breakdown"] = df.set_index("deviceCategory")[
            ["sessions", "totalUsers", "transactions"]
        ].to_dict("index")

    # 参照元/メディア（上位10）
    if "source_medium" in ga4_data:
        df = ga4_data["source_medium"].sort_values("sessions", ascending=False).head(10)
        metrics["top_sources"] = []
        for _, row in df.iterrows():
            metrics["top_sources"].append({
                "source": row["sessionSource"],
                "medium": row["sessionMedium"],
                "sessions": int(row["sessions"]),
                "users": int(row["totalUsers"]),
                "transactions": int(row["transactions"]),
                "revenue": row["totalRevenue"],
            })

    return metrics


def pct_change(current, previous):
    """変化率を計算"""
    if previous == 0:
        return None
    return (current - previous) / previous


def fmt_yen(amount):
    """金額をフォーマット"""
    return f"¥{amount:,.0f}"


def fmt_pct(ratio):
    """パーセントをフォーマット"""
    if ratio is None:
        return "N/A"
    return f"{ratio:+.1%}"


def fmt_pct_val(ratio):
    """パーセント値をフォーマット（符号なし）"""
    if ratio is None:
        return "N/A"
    return f"{ratio:.1%}"


def generate_report(this_sales, last_sales, this_ga4, last_ga4):
    """Markdownレポートを生成"""

    lines = []
    lines.append("# 週次レポート：今週 vs 先週")
    lines.append("")
    lines.append(f"- **今週**: {THIS_WEEK_START} 〜 {THIS_WEEK_END}")
    lines.append(f"- **先週**: {LAST_WEEK_START} 〜 {LAST_WEEK_END}")
    lines.append(f"- **レポート生成日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    # ── 1. 売上サマリー ──
    lines.append("---")
    lines.append("## 1. 売上サマリー")
    lines.append("")

    sales_change = pct_change(this_sales["total_sales"], last_sales["total_sales"])
    order_change = pct_change(this_sales["order_count"], last_sales["order_count"])
    aov_change = pct_change(this_sales["avg_order_value"], last_sales["avg_order_value"])

    lines.append("| 指標 | 先週 | 今週 | 変化率 |")
    lines.append("|------|------|------|--------|")
    lines.append(f"| 売上合計 | {fmt_yen(last_sales['total_sales'])} | {fmt_yen(this_sales['total_sales'])} | {fmt_pct(sales_change)} |")
    lines.append(f"| 注文数 | {last_sales['order_count']}件 | {this_sales['order_count']}件 | {fmt_pct(order_change)} |")
    lines.append(f"| 平均注文額（AOV） | {fmt_yen(last_sales['avg_order_value'])} | {fmt_yen(this_sales['avg_order_value'])} | {fmt_pct(aov_change)} |")
    lines.append(f"| 値引き合計 | {fmt_yen(last_sales['total_discount'])} | {fmt_yen(this_sales['total_discount'])} | {fmt_pct(pct_change(this_sales['total_discount'], last_sales['total_discount']))} |")
    lines.append(f"| 新規顧客率 | {fmt_pct_val(last_sales['new_customer_ratio'])} | {fmt_pct_val(this_sales['new_customer_ratio'])} | - |")
    lines.append("")

    # ── 2. 日別売上推移 ──
    lines.append("---")
    lines.append("## 2. 日別売上推移")
    lines.append("")

    lines.append("### 先週")
    lines.append("| 日付 | 注文数 | 売上 |")
    lines.append("|------|--------|------|")
    for date_str in sorted(last_sales["daily_sales"].keys()):
        d = last_sales["daily_sales"][date_str]
        lines.append(f"| {date_str} | {d['orders']}件 | {fmt_yen(d['sales'])} |")
    lines.append("")

    lines.append("### 今週")
    lines.append("| 日付 | 注文数 | 売上 |")
    lines.append("|------|--------|------|")
    for date_str in sorted(this_sales["daily_sales"].keys()):
        d = this_sales["daily_sales"][date_str]
        lines.append(f"| {date_str} | {d['orders']}件 | {fmt_yen(d['sales'])} |")
    lines.append("")

    # ── 3. 商品別売上 ──
    lines.append("---")
    lines.append("## 3. 商品別売上")
    lines.append("")

    all_products = set(list(this_sales["product_breakdown"].keys()) +
                       list(last_sales["product_breakdown"].keys()))

    lines.append("| 商品名 | 先週（数量/売上） | 今週（数量/売上） | 売上変化率 |")
    lines.append("|--------|------------------|------------------|-----------|")
    for product in sorted(all_products):
        last_p = last_sales["product_breakdown"].get(product, {"quantity": 0, "sales": 0})
        this_p = this_sales["product_breakdown"].get(product, {"quantity": 0, "sales": 0})
        change = pct_change(this_p["sales"], last_p["sales"])
        lines.append(
            f"| {product} | {last_p['quantity']}個 / {fmt_yen(last_p['sales'])} | "
            f"{this_p['quantity']}個 / {fmt_yen(this_p['sales'])} | {fmt_pct(change)} |"
        )
    lines.append("")

    # ── 4. ディスカウントコード別 ──
    lines.append("---")
    lines.append("## 4. ディスカウントコード利用状況")
    lines.append("")

    all_codes = set(list(this_sales["discount_breakdown"].keys()) +
                    list(last_sales["discount_breakdown"].keys()))
    if all_codes:
        lines.append("| コード | 先週（件数/金額） | 今週（件数/金額） |")
        lines.append("|--------|------------------|------------------|")
        for code in sorted(all_codes):
            last_d = last_sales["discount_breakdown"].get(code, {"count": 0, "total": 0})
            this_d = this_sales["discount_breakdown"].get(code, {"count": 0, "total": 0})
            lines.append(
                f"| {code} | {last_d['count']}件 / {fmt_yen(last_d['total'])} | "
                f"{this_d['count']}件 / {fmt_yen(this_d['total'])} |"
            )
    else:
        lines.append("ディスカウントコードの利用なし")
    lines.append("")

    # ── 5. GA4 トラフィック ──
    if this_ga4 and last_ga4:
        lines.append("---")
        lines.append("## 5. トラフィック（GA4）")
        lines.append("")

        if "total_sessions" in this_ga4 and "total_sessions" in last_ga4:
            sess_change = pct_change(this_ga4["total_sessions"], last_ga4["total_sessions"])
            user_change = pct_change(this_ga4["total_users"], last_ga4["total_users"])

            lines.append("| 指標 | 先週 | 今週 | 変化率 |")
            lines.append("|------|------|------|--------|")
            lines.append(f"| セッション数 | {last_ga4['total_sessions']:,} | {this_ga4['total_sessions']:,} | {fmt_pct(sess_change)} |")
            lines.append(f"| ユーザー数 | {last_ga4['total_users']:,} | {this_ga4['total_users']:,} | {fmt_pct(user_change)} |")
            lines.append(f"| 新規ユーザー | {last_ga4.get('new_users', 0):,} | {this_ga4.get('new_users', 0):,} | {fmt_pct(pct_change(this_ga4.get('new_users', 0), last_ga4.get('new_users', 0)))} |")

            if "cvr" in this_ga4 and "cvr" in last_ga4:
                lines.append(f"| CVR | {fmt_pct_val(last_ga4['cvr'])} | {fmt_pct_val(this_ga4['cvr'])} | {fmt_pct(pct_change(this_ga4['cvr'], last_ga4['cvr']))} |")

            if "avg_bounce_rate" in this_ga4 and "avg_bounce_rate" in last_ga4:
                lines.append(f"| 直帰率（平均） | {fmt_pct_val(last_ga4['avg_bounce_rate'])} | {fmt_pct_val(this_ga4['avg_bounce_rate'])} | - |")

            lines.append("")

        # ファネル
        if "total_add_to_carts" in this_ga4 and "total_add_to_carts" in last_ga4:
            lines.append("### CVRファネル")
            lines.append("")
            lines.append("| ステップ | 先週 | 今週 | 変化率 |")
            lines.append("|----------|------|------|--------|")
            lines.append(f"| セッション | {last_ga4['total_sessions']:,} | {this_ga4['total_sessions']:,} | {fmt_pct(pct_change(this_ga4['total_sessions'], last_ga4['total_sessions']))} |")
            lines.append(f"| カート追加 | {last_ga4['total_add_to_carts']:,} | {this_ga4['total_add_to_carts']:,} | {fmt_pct(pct_change(this_ga4['total_add_to_carts'], last_ga4['total_add_to_carts']))} |")
            lines.append(f"| チェックアウト | {last_ga4['total_checkouts']:,} | {this_ga4['total_checkouts']:,} | {fmt_pct(pct_change(this_ga4['total_checkouts'], last_ga4['total_checkouts']))} |")
            lines.append(f"| 購入完了 | {last_ga4['total_transactions']:,} | {this_ga4['total_transactions']:,} | {fmt_pct(pct_change(this_ga4['total_transactions'], last_ga4['total_transactions']))} |")
            if "cart_rate" in this_ga4:
                lines.append(f"| カート追加率 | {fmt_pct_val(last_ga4.get('cart_rate', 0))} | {fmt_pct_val(this_ga4.get('cart_rate', 0))} | - |")
            if "checkout_rate" in this_ga4:
                lines.append(f"| チェックアウト率 | {fmt_pct_val(last_ga4.get('checkout_rate', 0))} | {fmt_pct_val(this_ga4.get('checkout_rate', 0))} | - |")
            lines.append("")

        # チャネル別
        if "channel_breakdown" in this_ga4 and "channel_breakdown" in last_ga4:
            lines.append("### チャネル別セッション")
            lines.append("")
            lines.append("| チャネル | 先週 | 今週 | 変化率 |")
            lines.append("|----------|------|------|--------|")
            all_channels = set(list(this_ga4["channel_breakdown"].keys()) +
                               list(last_ga4["channel_breakdown"].keys()))
            for ch in sorted(all_channels, key=lambda c: this_ga4["channel_breakdown"].get(c, {}).get("sessions", 0), reverse=True):
                last_s = last_ga4["channel_breakdown"].get(ch, {}).get("sessions", 0)
                this_s = this_ga4["channel_breakdown"].get(ch, {}).get("sessions", 0)
                change = pct_change(this_s, last_s)
                lines.append(f"| {ch} | {int(last_s):,} | {int(this_s):,} | {fmt_pct(change)} |")
            lines.append("")

        # デバイス別
        if "device_breakdown" in this_ga4 and "device_breakdown" in last_ga4:
            lines.append("### デバイス別セッション")
            lines.append("")
            lines.append("| デバイス | 先週 | 今週 | 変化率 |")
            lines.append("|----------|------|------|--------|")
            all_devices = set(list(this_ga4["device_breakdown"].keys()) +
                              list(last_ga4["device_breakdown"].keys()))
            for dev in sorted(all_devices):
                last_s = last_ga4["device_breakdown"].get(dev, {}).get("sessions", 0)
                this_s = this_ga4["device_breakdown"].get(dev, {}).get("sessions", 0)
                change = pct_change(this_s, last_s)
                lines.append(f"| {dev} | {int(last_s):,} | {int(this_s):,} | {fmt_pct(change)} |")
            lines.append("")

        # 参照元トップ10
        if "top_sources" in this_ga4:
            lines.append("### 参照元トップ10（今週）")
            lines.append("")
            lines.append("| 参照元 | メディア | セッション | ユーザー | 購入数 | 売上 |")
            lines.append("|--------|----------|-----------|---------|--------|------|")
            for src in this_ga4["top_sources"]:
                lines.append(
                    f"| {src['source']} | {src['medium']} | {src['sessions']:,} | "
                    f"{src['users']:,} | {src['transactions']} | {fmt_yen(src['revenue'])} |"
                )
            lines.append("")

    # ── 6. まとめ ──
    lines.append("---")
    lines.append("## まとめ・所感")
    lines.append("")
    lines.append("（分析コメントは別途追記）")
    lines.append("")

    return "\n".join(lines)


def main():
    print("=" * 50)
    print("週次比較分析: 今週 vs 先週")
    print("=" * 50)
    print()

    # 1. Shopify注文データ取得
    print("[1/4] Shopify注文データ取得")
    last_orders = fetch_orders(LAST_WEEK_START, LAST_WEEK_END)
    this_orders = fetch_orders(THIS_WEEK_START, THIS_WEEK_END)
    print()

    # 2. 売上KPI計算
    print("[2/4] 売上KPI計算")
    last_sales = calc_sales_metrics(last_orders, "先週")
    this_sales = calc_sales_metrics(this_orders, "今週")
    print(f"  先週: {last_sales['order_count']}件 / {fmt_yen(last_sales['total_sales'])}")
    print(f"  今週: {this_sales['order_count']}件 / {fmt_yen(this_sales['total_sales'])}")
    print()

    # 3. GA4データ取得
    print("[3/4] GA4トラフィックデータ取得")
    last_ga4_raw = fetch_ga4_data(LAST_WEEK_START, LAST_WEEK_END)
    this_ga4_raw = fetch_ga4_data(THIS_WEEK_START, THIS_WEEK_END)
    last_ga4 = calc_ga4_metrics(last_ga4_raw, "先週")
    this_ga4 = calc_ga4_metrics(this_ga4_raw, "今週")
    print()

    # 4. レポート生成
    print("[4/4] レポート生成")
    report = generate_report(this_sales, last_sales, this_ga4, last_ga4)

    os.makedirs("output/weekly", exist_ok=True)
    report_path = "output/weekly/weekly_report_20260314.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"  レポート保存: {report_path}")
    print()

    # コンソールにもサマリー出力
    print("=" * 50)
    print("サマリー")
    print("=" * 50)
    sales_change = pct_change(this_sales["total_sales"], last_sales["total_sales"])
    order_change = pct_change(this_sales["order_count"], last_sales["order_count"])
    print(f"売上: {fmt_yen(last_sales['total_sales'])} → {fmt_yen(this_sales['total_sales'])} ({fmt_pct(sales_change)})")
    print(f"注文: {last_sales['order_count']}件 → {this_sales['order_count']}件 ({fmt_pct(order_change)})")
    if "total_sessions" in this_ga4 and "total_sessions" in last_ga4:
        sess_change = pct_change(this_ga4["total_sessions"], last_ga4["total_sessions"])
        print(f"セッション: {last_ga4['total_sessions']:,} → {this_ga4['total_sessions']:,} ({fmt_pct(sess_change)})")
    if "cvr" in this_ga4 and "cvr" in last_ga4:
        print(f"CVR: {fmt_pct_val(last_ga4['cvr'])} → {fmt_pct_val(this_ga4['cvr'])}")

    # JSON形式でも保存（後続分析用）
    summary_data = {
        "this_week": {"sales": this_sales, "ga4": this_ga4},
        "last_week": {"sales": last_sales, "ga4": last_ga4},
    }
    with open("output/weekly/weekly_data_20260314.json", "w", encoding="utf-8") as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nデータJSON保存: output/weekly/weekly_data_20260314.json")


if __name__ == "__main__":
    main()
