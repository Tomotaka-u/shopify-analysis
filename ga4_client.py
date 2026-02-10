"""
GA4 API クライアント
- サービスアカウント認証でGA4 Data APIに接続
- Claude Codeから呼び出して使う
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
GA4_CREDENTIALS_PATH = os.getenv("GA4_CREDENTIALS_PATH", "credentials.json")

def get_ga4_client():
    """GA4 Data APIクライアントを初期化"""
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_file(
        GA4_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def run_report(
    dimensions: list[str],
    metrics: list[str],
    start_date: str = "30daysAgo",
    end_date: str = "today",
    dimension_filter=None,
    order_bys=None,
    limit: int = 10000
) -> pd.DataFrame:
    """
    GA4レポートを実行してDataFrameで返す

    Parameters:
        dimensions: ディメンション名のリスト（例：["date", "sessionSource"]）
        metrics: メトリクス名のリスト（例：["sessions", "transactions"]）
        start_date: 開始日（"2024-01-01" or "30daysAgo"）
        end_date: 終了日（"2024-12-31" or "today"）
        dimension_filter: フィルター条件（オプション）
        order_bys: 並び替え条件（オプション）
        limit: 最大行数（デフォルト10000）

    Returns:
        pd.DataFrame: レポート結果

    使用例:
        # 日別セッション数
        df = run_report(["date"], ["sessions"], "30daysAgo", "today")

        # チャネル別の流入とCVR
        df = run_report(
            ["sessionDefaultChannelGroup"],
            ["sessions", "transactions", "transactionRevenue"],
            "2024-01-01", "2024-12-31"
        )

        # ページ別のPV
        df = run_report(["pagePath"], ["screenPageViews"], "7daysAgo", "today")
    """
    from google.analytics.data_v1beta.types import (
        RunReportRequest, DateRange, Dimension, Metric,
        FilterExpression, Filter, OrderBy
    )

    client = get_ga4_client()

    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit
    )

    if dimension_filter:
        request.dimension_filter = dimension_filter

    if order_bys:
        request.order_bys = order_bys

    response = client.run_report(request)

    # DataFrameに変換
    rows = []
    for row in response.rows:
        dim_values = [v.value for v in row.dimension_values]
        met_values = [v.value for v in row.metric_values]
        rows.append(dim_values + met_values)

    columns = [d.name for d in response.dimension_headers] + \
              [m.name for m in response.metric_headers]

    df = pd.DataFrame(rows, columns=columns)

    # 数値型に変換（メトリクスのカラム）
    for col in [m.name for m in response.metric_headers]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # dateカラムがあれば日付型に変換
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

    return df


def get_traffic_overview(start_date="30daysAgo", end_date="today"):
    """トラフィック概要（チャネル別セッション・ユーザー数）"""
    return run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions", "totalUsers", "newUsers", "bounceRate",
                 "averageSessionDuration", "screenPageViewsPerSession"],
        start_date=start_date,
        end_date=end_date
    )


def get_daily_traffic(start_date="30daysAgo", end_date="today"):
    """日別トラフィック推移"""
    return run_report(
        dimensions=["date"],
        metrics=["sessions", "totalUsers", "transactions", "totalRevenue"],
        start_date=start_date,
        end_date=end_date
    )


def get_source_medium(start_date="30daysAgo", end_date="today"):
    """参照元/メディア別のトラフィック"""
    return run_report(
        dimensions=["sessionSource", "sessionMedium"],
        metrics=["sessions", "totalUsers", "transactions", "totalRevenue"],
        start_date=start_date,
        end_date=end_date
    )


def get_landing_pages(start_date="30daysAgo", end_date="today"):
    """ランディングページ別のパフォーマンス"""
    return run_report(
        dimensions=["landingPage"],
        metrics=["sessions", "totalUsers", "bounceRate",
                 "averageSessionDuration", "transactions"],
        start_date=start_date,
        end_date=end_date
    )


def get_device_breakdown(start_date="30daysAgo", end_date="today"):
    """デバイス別のトラフィック"""
    return run_report(
        dimensions=["deviceCategory"],
        metrics=["sessions", "totalUsers", "transactions",
                 "totalRevenue", "bounceRate"],
        start_date=start_date,
        end_date=end_date
    )


def get_geo_data(start_date="30daysAgo", end_date="today"):
    """地域別のトラフィック"""
    return run_report(
        dimensions=["country", "city"],
        metrics=["sessions", "totalUsers", "transactions"],
        start_date=start_date,
        end_date=end_date
    )


def get_page_views(start_date="30daysAgo", end_date="today"):
    """ページ別のPV・滞在時間"""
    return run_report(
        dimensions=["pagePath", "pageTitle"],
        metrics=["screenPageViews", "totalUsers",
                 "averageSessionDuration", "bounceRate"],
        start_date=start_date,
        end_date=end_date
    )


# --- テスト実行 ---
if __name__ == "__main__":
    print("=== GA4 API 接続テスト ===\n")

    try:
        df = get_traffic_overview("7daysAgo", "today")
        print("✅ 接続成功！直近7日間のトラフィック概要：\n")
        print(df.to_string(index=False))
        print(f"\n合計セッション数: {df['sessions'].sum():,.0f}")
        print(f"合計ユーザー数: {df['totalUsers'].sum():,.0f}")
    except Exception as e:
        print(f"❌ エラー: {e}")
        print("\n確認事項:")
        print("1. .env に GA4_PROPERTY_ID が設定されているか")
        print("2. credentials.json がプロジェクトフォルダにあるか")
        print("3. サービスアカウントがGA4プロパティに追加されているか")
        print("4. Google Analytics Data API が有効化されているか")