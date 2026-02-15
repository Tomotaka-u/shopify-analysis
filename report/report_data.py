"""
データ処理モジュール
CSVファイルとGA4 APIからデータを読み込み、テンプレートに渡す辞書を構築する。
"""

from __future__ import annotations

import os
from datetime import datetime, date, timedelta

import pandas as pd

from report_config import ComparisonConfig, Campaign

# プロジェクトルート（data/ ディレクトリの基準）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _safe_read_csv(path: str) -> pd.DataFrame | None:
    """CSVが存在すれば読み込み、なければNoneを返す"""
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


def _campaign_data_path(campaign: Campaign, filename: str) -> str:
    return os.path.join(_PROJECT_ROOT, "data", campaign.data_dir, filename)


# ──────────────────────────────────────────────
#  Orders 分析
# ──────────────────────────────────────────────

def _filter_orders(df: pd.DataFrame, campaign: Campaign) -> pd.DataFrame:
    """orders.csv をキャンペーン期間でフィルタ"""
    df = df.copy()
    df["created_dt"] = pd.to_datetime(df["Created at"])
    df["date"] = df["created_dt"].dt.date
    mask = (df["date"] >= campaign.start_date) & (df["date"] <= campaign.end_date)
    return df[mask].copy()


def _dedup_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """注文名で重複排除（複数ラインアイテムの注文を1行にまとめる）"""
    return orders.drop_duplicates(subset="Name", keep="first")


def _calc_sales_metrics(orders: pd.DataFrame, campaign: Campaign) -> dict:
    """基本売上指標を算出"""
    unique = _dedup_orders(orders)
    total_sales = unique["Total"].sum()
    order_count = len(unique)
    aov = total_sales / order_count if order_count else 0
    median_order = unique["Total"].median() if order_count else 0
    daily_orders = order_count / campaign.days
    daily_sales = total_sales / campaign.days

    return {
        "total_sales": total_sales,
        "order_count": order_count,
        "aov": aov,
        "median_order": median_order,
        "daily_orders": daily_orders,
        "daily_sales": daily_sales,
        "days": campaign.days,
    }


def _calc_discount_metrics(orders: pd.DataFrame) -> dict:
    """割引分析"""
    unique = _dedup_orders(orders)
    total = len(unique)
    if total == 0:
        return {"discount_rate": 0, "avg_discount": 0, "no_code_count": 0, "no_code_pct": 0}

    has_code = unique["Discount Code"].notna() & (unique["Discount Code"] != "")
    no_code_count = (~has_code).sum()
    discount_rate = has_code.sum() / total
    avg_discount = unique.loc[has_code, "Discount Amount"].mean() if has_code.any() else 0

    return {
        "discount_rate": discount_rate,
        "avg_discount": avg_discount,
        "no_code_count": no_code_count,
        "no_code_pct": no_code_count / total,
    }


def _calc_affiliate_ranking(orders: pd.DataFrame, top_n: int = 8) -> list[dict]:
    """割引コード別のランキング"""
    df = _dedup_orders(orders).copy()
    df["code"] = df["Discount Code"].fillna("").replace("", "(コードなし)")

    grouped = df.groupby("code").agg(
        order_count=("Total", "size"),
        total_sales=("Total", "sum"),
    ).sort_values("order_count", ascending=False).head(top_n)

    ranking = []
    for i, (code, row) in enumerate(grouped.iterrows(), 1):
        ranking.append({
            "rank": i,
            "name": code if code != "(コードなし)" else "（コードなし）",
            "orders": int(row["order_count"]),
            "sales": row["total_sales"],
        })
    return ranking


def _calc_product_breakdown(orders: pd.DataFrame) -> list[dict]:
    """商品別の販売数・構成比（orders.csv のLineitemから算出）"""
    df = orders.copy()
    grouped = df.groupby("Lineitem name").agg(
        qty=("Lineitem quantity", "sum"),
    ).sort_values("qty", ascending=False)

    total_qty = grouped["qty"].sum()
    result = []
    for name, row in grouped.iterrows():
        result.append({
            "name": name,
            "qty": int(row["qty"]),
            "pct": row["qty"] / total_qty if total_qty else 0,
        })
    return result


def _calc_time_blocks(orders: pd.DataFrame) -> list[dict]:
    """時間帯ブロック別の注文構成比"""
    df = _dedup_orders(orders).copy()
    df["hour"] = df["created_dt"].dt.hour
    total = len(df)

    blocks = [
        ("深夜（0-5時）", range(0, 6)),
        ("午前（6-11時）", range(6, 12)),
        ("午後（12-17時）", range(12, 18)),
        ("夜（18-23時）", range(18, 24)),
    ]
    result = []
    for label, hours in blocks:
        count = df[df["hour"].isin(hours)].shape[0]
        result.append({
            "label": label,
            "count": count,
            "pct": count / total if total else 0,
            "highlight": label.startswith("夜"),
        })
    return result


def _calc_day_of_week(orders: pd.DataFrame) -> list[dict]:
    """曜日別の注文数"""
    df = _dedup_orders(orders).copy()
    df["dow"] = df["created_dt"].dt.dayofweek  # 0=Mon
    total = len(df)

    day_names = ["月曜", "火曜", "水曜", "木曜", "金曜", "土曜", "日曜"]
    grouped = df.groupby("dow").agg(
        count=("Total", "size"),
        avg_order=("Total", "mean"),
    )

    # 日曜が最初に来るようにソート (多い順)
    result = []
    for dow in range(7):
        if dow in grouped.index:
            row = grouped.loc[dow]
            result.append({
                "label": day_names[dow],
                "count": int(row["count"]),
                "pct": row["count"] / total if total else 0,
                "avg_order": row["avg_order"],
            })
    # 件数で降順ソート
    result.sort(key=lambda x: x["count"], reverse=True)
    return result


def _find_peak_day(orders: pd.DataFrame) -> dict | None:
    """ピーク日（注文数最多日）を特定"""
    if orders.empty:
        return None
    unique = _dedup_orders(orders)
    daily = unique.groupby("date").agg(
        order_count=("Total", "size"),
        total_sales=("Total", "sum"),
    )
    peak_date = daily["order_count"].idxmax()
    peak = daily.loc[peak_date]
    return {
        "date": peak_date,
        "order_count": int(peak["order_count"]),
        "total_sales": peak["total_sales"],
    }


# ──────────────────────────────────────────────
#  CVR ファネル
# ──────────────────────────────────────────────

def _load_funnel(campaign: Campaign) -> dict | None:
    """cvr_breakdown.csv からファネルデータを読み込む"""
    path = _campaign_data_path(campaign, "cvr_breakdown.csv")
    df = _safe_read_csv(path)
    if df is None:
        return None

    # 有効な行（Sessionsが0でない）だけ
    df = df[df["Sessions"] > 0].copy()

    sessions = int(df["Sessions"].sum())
    cart = int(df["Sessions with cart additions"].sum())
    checkout = int(df["Sessions that reached checkout"].sum())
    purchase = int(df["Sessions that completed checkout"].sum())

    cvr = purchase / sessions if sessions else 0

    steps = [
        {"label": "セッション", "count": sessions, "rate_from_top": 1.0},
        {"label": "カート追加", "count": cart, "rate_from_top": cart / sessions if sessions else 0},
        {"label": "チェックアウト到達", "count": checkout, "rate_from_top": checkout / sessions if sessions else 0},
        {"label": "購入完了", "count": purchase, "rate_from_top": cvr},
    ]

    # ステップ間の遷移率を計算
    for i in range(1, len(steps)):
        prev = steps[i - 1]["count"]
        steps[i]["step_rate"] = steps[i]["count"] / prev if prev else 0

    steps[0]["step_rate"] = None  # 最初のステップには遷移率なし

    return {
        "sessions": sessions,
        "cart": cart,
        "checkout": checkout,
        "purchase": purchase,
        "cvr": cvr,
        "steps": steps,
        "daily": df[["Day", "Sessions", "Sessions with cart additions",
                      "Sessions that completed checkout", "Conversion rate"]].to_dict("records"),
    }


# ──────────────────────────────────────────────
#  ソーシャル
# ──────────────────────────────────────────────

def _load_social_sales(campaign: Campaign) -> list[dict]:
    """sales_by_social_referrer.csv を読み込む"""
    path = _campaign_data_path(campaign, "sales_by_social_referrer.csv")
    df = _safe_read_csv(path)
    if df is None:
        return []
    result = []
    for _, row in df.iterrows():
        result.append({
            "name": row["Order referrer name"],
            "sales": row["Total sales"],
        })
    return result


def _load_all_social_sales(csv_path: str | None) -> list[dict]:
    """全期間のソーシャルリファラー売上"""
    if csv_path is None:
        return []
    df = _safe_read_csv(os.path.join(_PROJECT_ROOT, csv_path))
    if df is None:
        return []
    result = []
    for _, row in df.iterrows():
        result.append({
            "name": row["Order referrer name"],
            "sales": row["Total sales"],
        })
    return result


# ──────────────────────────────────────────────
#  商品（CSV版）
# ──────────────────────────────────────────────

def _load_product_sales_csv(campaign: Campaign) -> list[dict]:
    """sales_by_product.csv を読み込む"""
    path = _campaign_data_path(campaign, "sales_by_product.csv")
    df = _safe_read_csv(path)
    if df is None:
        return []
    total_qty = df["Net items sold"].sum()
    result = []
    for _, row in df.iterrows():
        result.append({
            "name": row["Product title"],
            "qty": int(row["Net items sold"]),
            "pct": row["Net items sold"] / total_qty if total_qty else 0,
            "gross_sales": row.get("Gross sales", 0),
            "net_sales": row.get("Net sales", 0),
            "total_sales": row.get("Total sales", 0),
        })
    return result


# ──────────────────────────────────────────────
#  GA4
# ──────────────────────────────────────────────

def _load_ga4_data(campaign: Campaign) -> dict | None:
    """GA4 APIからトラフィックデータを取得"""
    try:
        from ga4_client import (
            get_traffic_overview,
            get_source_medium,
        )
    except ImportError:
        return None

    try:
        overview = get_traffic_overview(campaign.ga4_start, campaign.ga4_end)
        source_medium = get_source_medium(campaign.ga4_start, campaign.ga4_end)
    except Exception:
        return None

    total_sessions = int(overview["sessions"].sum())
    total_users = int(overview["totalUsers"].sum())
    new_users = int(overview["newUsers"].sum())
    daily_sessions = total_sessions / campaign.days

    # チャネル別セッション
    channels = []
    for _, row in overview.sort_values("sessions", ascending=False).iterrows():
        channels.append({
            "name": row["sessionDefaultChannelGroup"],
            "sessions": int(row["sessions"]),
            "pct": int(row["sessions"]) / total_sessions if total_sessions else 0,
        })

    # ソース/メディア別
    sources = []
    for _, row in source_medium.sort_values("sessions", ascending=False).head(20).iterrows():
        sources.append({
            "source": row["sessionSource"],
            "medium": row["sessionMedium"],
            "sessions": int(row["sessions"]),
            "users": int(row["totalUsers"]),
        })

    return {
        "total_sessions": total_sessions,
        "total_users": total_users,
        "new_users": new_users,
        "new_user_rate": new_users / total_users if total_users else 0,
        "daily_sessions": daily_sessions,
        "channels": channels,
        "sources": sources,
    }


# ──────────────────────────────────────────────
#  メイン関数
# ──────────────────────────────────────────────

def load_report_data(config: ComparisonConfig, use_ga4: bool = True) -> dict:
    """
    テンプレートに渡す全データを構築して返す。

    Returns:
        dict: テンプレートに渡すコンテキスト辞書
    """
    a = config.campaign_a
    b = config.campaign_b

    # Orders読み込み
    all_orders = pd.read_csv(os.path.join(_PROJECT_ROOT, config.orders_csv))
    orders_a = _filter_orders(all_orders, a)
    orders_b = _filter_orders(all_orders, b)

    # 売上指標
    sales_a = _calc_sales_metrics(orders_a, a)
    sales_b = _calc_sales_metrics(orders_b, b)

    # 比率計算ヘルパー
    def ratio(va, vb):
        if vb and vb != 0:
            return va / vb
        return None

    # KPI
    kpis = [
        {
            "label": "総売上",
            "value_a": sales_a["total_sales"],
            "value_b": sales_b["total_sales"],
            "fmt": "yen",
        },
        {
            "label": "注文数",
            "value_a": sales_a["order_count"],
            "value_b": sales_b["order_count"],
            "fmt": "count",
            "suffix": "件",
        },
        {
            "label": "平均注文額",
            "value_a": sales_a["aov"],
            "value_b": sales_b["aov"],
            "fmt": "yen",
        },
    ]

    # CVRファネル
    funnel_a = _load_funnel(a)
    funnel_b = _load_funnel(b)

    # CVR KPI追加
    if funnel_a and funnel_b:
        kpis.append({
            "label": "CVR（Shopify）",
            "value_a": funnel_a["cvr"],
            "value_b": funnel_b["cvr"],
            "fmt": "pct",
        })

    # 売上比較テーブル
    sales_ratio_orders = ratio(sales_a["order_count"], sales_b["order_count"])
    sales_ratio_sales = ratio(sales_a["total_sales"], sales_b["total_sales"])
    sales_ratio_daily_orders = ratio(sales_a["daily_orders"], sales_b["daily_orders"])
    sales_ratio_daily_sales = ratio(sales_a["daily_sales"], sales_b["daily_sales"])

    sales_table = [
        {"label": "期間", "a": f"{a.days}日間", "b": f"{b.days}日間", "ratio": "-"},
        {"label": "注文数", "a": sales_a["order_count"], "b": sales_b["order_count"],
         "ratio": f"{sales_ratio_orders:.1f}倍" if sales_ratio_orders else "-", "fmt": "count", "suffix": "件"},
        {"label": "売上合計", "a": sales_a["total_sales"], "b": sales_b["total_sales"],
         "ratio": f"{sales_ratio_sales:.1f}倍" if sales_ratio_sales else "-", "fmt": "yen"},
        {"label": "1日あたり注文", "a": sales_a["daily_orders"], "b": sales_b["daily_orders"],
         "ratio": f"{sales_ratio_daily_orders:.1f}倍" if sales_ratio_daily_orders else "-", "fmt": "count_1f", "suffix": "件"},
        {"label": "1日あたり売上", "a": sales_a["daily_sales"], "b": sales_b["daily_sales"],
         "ratio": f"{sales_ratio_daily_sales:.1f}倍" if sales_ratio_daily_sales else "-", "fmt": "yen"},
        {"label": "平均注文額", "a": sales_a["aov"], "b": sales_b["aov"], "ratio": "-", "fmt": "yen"},
        {"label": "中央値注文額", "a": sales_a["median_order"], "b": sales_b["median_order"], "ratio": "-", "fmt": "yen"},
    ]

    # 割引分析
    discount_a = _calc_discount_metrics(orders_a)
    discount_b = _calc_discount_metrics(orders_b)

    discount_table = [
        {"label": "割引適用率", "a": discount_a["discount_rate"], "b": discount_b["discount_rate"], "fmt": "pct"},
        {"label": "平均割引額", "a": discount_a["avg_discount"], "b": discount_b["avg_discount"], "fmt": "yen"},
        {"label": "コードなし購入", "a": discount_a["no_code_count"], "b": discount_b["no_code_count"],
         "fmt": "custom",
         "a_display": f"{discount_a['no_code_count']}件（{discount_a['no_code_pct']:.1%}）",
         "b_display": f"{discount_b['no_code_count']}件（{discount_b['no_code_pct']:.1%}）"},
    ]

    # アフィリエイトランキング
    affiliate_a = _calc_affiliate_ranking(orders_a)
    affiliate_b = _calc_affiliate_ranking(orders_b)

    # アフィリエイター間の比較テーブル（主要コード）
    codes_a = {r["name"]: r for r in affiliate_a}
    codes_b = {r["name"]: r for r in affiliate_b}
    all_codes = set(codes_a.keys()) | set(codes_b.keys())
    # 「（コードなし）」は除外
    all_codes.discard("（コードなし）")
    # 合計件数で降順ソート
    sorted_codes = sorted(all_codes,
                          key=lambda c: (codes_a.get(c, {}).get("orders", 0) +
                                         codes_b.get(c, {}).get("orders", 0)),
                          reverse=True)

    affiliate_comparison = []
    for code in sorted_codes[:8]:
        ca = codes_a.get(code, {})
        cb = codes_b.get(code, {})
        affiliate_comparison.append({
            "name": code,
            "orders_a": ca.get("orders", 0),
            "orders_b": cb.get("orders", 0),
        })

    # 商品分析
    products_a_csv = _load_product_sales_csv(a)
    products_b_csv = _load_product_sales_csv(b)

    # CSV版があればそれを使い、なければordersから算出
    if products_a_csv:
        products_a = products_a_csv
    else:
        products_a = _calc_product_breakdown(orders_a)

    if products_b_csv:
        products_b = products_b_csv
    else:
        products_b = _calc_product_breakdown(orders_b)

    # 商品テーブル（両セール合わせて表示）
    product_names_a = {p["name"]: p for p in products_a}
    product_names_b = {p["name"]: p for p in products_b}
    all_product_names = list(dict.fromkeys(
        [p["name"] for p in products_a] + [p["name"] for p in products_b]
    ))
    product_table = []
    for name in all_product_names:
        pa = product_names_a.get(name, {"qty": 0, "pct": 0})
        pb = product_names_b.get(name, {"qty": 0, "pct": 0})
        product_table.append({
            "name": name,
            "qty_a": pa["qty"],
            "pct_a": pa["pct"],
            "qty_b": pb["qty"],
            "pct_b": pb["pct"],
        })

    # ソーシャル売上
    social_a = _load_social_sales(a)
    social_b = _load_social_sales(b)
    all_social = _load_all_social_sales(config.all_social_csv)

    # ソーシャル売上テーブル
    social_names_a = {s["name"]: s for s in social_a}
    social_names_b = {s["name"]: s for s in social_b}
    social_names_all = {s["name"]: s for s in all_social}
    all_social_names = list(dict.fromkeys(
        [s["name"] for s in social_a] + [s["name"] for s in social_b]
    ))
    social_table = []
    for name in all_social_names:
        sa = social_names_a.get(name, {"sales": 0})
        sb = social_names_b.get(name, {"sales": 0})
        sall = social_names_all.get(name, {"sales": 0})
        social_table.append({
            "name": name.capitalize() if name else name,
            "sales_a": sa["sales"],
            "sales_b": sb["sales"],
            "sales_all": sall["sales"],
        })
    # 合計行
    social_table.append({
        "name": "合計",
        "sales_a": sum(s.get("sales", 0) for s in social_a),
        "sales_b": sum(s.get("sales", 0) for s in social_b),
        "sales_all": sum(s.get("sales", 0) for s in all_social),
    })

    # 時間帯・曜日（全期間）
    all_orders_parsed = all_orders.copy()
    all_orders_parsed["created_dt"] = pd.to_datetime(all_orders_parsed["Created at"])
    time_blocks_all = _calc_time_blocks(all_orders_parsed)
    time_blocks_a = _calc_time_blocks(orders_a)
    time_blocks_b = _calc_time_blocks(orders_b)

    # 時間帯テーブル結合
    time_table = []
    for i, block in enumerate(time_blocks_all):
        time_table.append({
            "label": block["label"],
            "count_all": block["count"],
            "pct_all": block["pct"],
            "count_a": time_blocks_a[i]["count"],
            "pct_a": time_blocks_a[i]["pct"],
            "count_b": time_blocks_b[i]["count"],
            "pct_b": time_blocks_b[i]["pct"],
            "highlight": block["highlight"],
        })

    dow_all = _calc_day_of_week(all_orders_parsed)

    # ピーク日
    peak_a = _find_peak_day(orders_a)
    peak_b = _find_peak_day(orders_b)

    # GA4データ
    ga4_a = None
    ga4_b = None
    if use_ga4:
        ga4_a = _load_ga4_data(a)
        ga4_b = _load_ga4_data(b)

    # GA4トラフィック概要テーブル
    traffic_table = None
    if ga4_a and ga4_b:
        r_sessions = ratio(ga4_a["total_sessions"], ga4_b["total_sessions"])
        r_users = ratio(ga4_a["total_users"], ga4_b["total_users"])
        r_new = ratio(ga4_a["new_users"], ga4_b["new_users"])
        r_daily = ratio(ga4_a["daily_sessions"], ga4_b["daily_sessions"])

        traffic_table = [
            {"label": "GA4 セッション",
             "a": ga4_a["total_sessions"], "b": ga4_b["total_sessions"],
             "ratio": f"{r_sessions:.1f}倍" if r_sessions else "-", "fmt": "num"},
            {"label": "ユニークユーザー",
             "a": ga4_a["total_users"], "b": ga4_b["total_users"],
             "ratio": f"{r_users:.1f}倍" if r_users else "-", "fmt": "num"},
            {"label": "新規ユーザー",
             "a": ga4_a["new_users"], "b": ga4_b["new_users"],
             "ratio": f"{r_new:.1f}倍" if r_new else "-", "fmt": "num"},
            {"label": "新規率",
             "a": ga4_a["new_user_rate"], "b": ga4_b["new_user_rate"],
             "ratio": "-", "fmt": "pct"},
            {"label": "1日あたりセッション",
             "a": ga4_a["daily_sessions"], "b": ga4_b["daily_sessions"],
             "ratio": f"{r_daily:.1f}倍" if r_daily else "-", "fmt": "num_int"},
        ]

    # チャネル別バーチャートデータ
    channel_bars = None
    if ga4_a and ga4_b:
        # 両方のチャネルを統合
        channels_a = {c["name"]: c for c in ga4_a["channels"]}
        channels_b = {c["name"]: c for c in ga4_b["channels"]}
        all_channel_names = list(dict.fromkeys(
            [c["name"] for c in ga4_a["channels"]] + [c["name"] for c in ga4_b["channels"]]
        ))
        max_sessions = max(
            max((c["sessions"] for c in ga4_a["channels"]), default=1),
            max((c["sessions"] for c in ga4_b["channels"]), default=1),
        )
        channel_bars = []
        for name in all_channel_names[:6]:
            ca = channels_a.get(name, {"sessions": 0, "pct": 0})
            cb = channels_b.get(name, {"sessions": 0, "pct": 0})
            channel_bars.append({
                "name": name,
                "sessions_a": ca["sessions"],
                "pct_a": ca["pct"],
                "width_a": ca["sessions"] / max_sessions * 100 if max_sessions else 0,
                "sessions_b": cb["sessions"],
                "pct_b": cb["pct"],
                "width_b": cb["sessions"] / max_sessions * 100 if max_sessions else 0,
            })

    # コンテキスト辞書
    today_str = date.today().isoformat()

    ctx = {
        # メタ情報
        "title": config.title,
        "sidebar_title": config.sidebar_title,
        "generated_date": today_str,
        "data_sources": "Shopify CSV + GA4 API" if use_ga4 else "Shopify CSV",

        # キャンペーン情報
        "campaign_a": {
            "name": a.name,
            "short_name": a.short_name,
            "css_class": a.css_class,
            "color_var": a.color_var,
            "period_label": a.period_label,
            "days": a.days,
            "start_date": a.start_date.isoformat(),
            "end_date": a.end_date.isoformat(),
        },
        "campaign_b": {
            "name": b.name,
            "short_name": b.short_name,
            "css_class": b.css_class,
            "color_var": b.color_var,
            "period_label": b.period_label,
            "days": b.days,
            "start_date": b.start_date.isoformat(),
            "end_date": b.end_date.isoformat(),
        },

        # KPI
        "kpis": kpis,

        # 売上テーブル
        "sales_table": sales_table,
        "discount_table": discount_table,

        # CVRファネル
        "funnel_a": funnel_a,
        "funnel_b": funnel_b,

        # トラフィック（GA4）
        "traffic_table": traffic_table,
        "channel_bars": channel_bars,
        "has_ga4": ga4_a is not None and ga4_b is not None,

        # ソーシャル
        "social_table": social_table,

        # 商品
        "product_table": product_table,

        # アフィリエイト
        "affiliate_a": affiliate_a,
        "affiliate_b": affiliate_b,
        "affiliate_comparison": affiliate_comparison,

        # ピーク日
        "peak_a": peak_a,
        "peak_b": peak_b,

        # 時間帯・曜日
        "time_table": time_table,
        "total_orders_count": len(_dedup_orders(all_orders_parsed)),
        "dow_table": dow_all,

        # インサイト（オプション）
        "insights": config.insights,
    }

    return ctx
