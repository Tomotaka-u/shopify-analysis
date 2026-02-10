# Shopify マーケティング分析エージェント

## プロジェクト概要
Shopifyでデジタル英語教材を販売するストアのマーケティングデータ分析プロジェクト。
shopify_client.py と ga4_client.py を使ってShopify API・GA4 APIからデータを取得し、分析・可視化を行う。

## ビジネスコンテキスト
- デジタル英語教材をShopifyで販売（1年以上運営）
- 商品数：5個以下（少数精鋭のラインナップ）
- 主な集客：SNS（Instagram, X）
- 在庫コストゼロ（デジタル商品）
- 日本市場向け

## 使えるツール
- `shopify_client.py`: Shopify APIクライアント
  - `get_orders(start_date, end_date)`: 注文データ取得
  - `get_customers()`: 顧客データ取得
  - `get_products()`: 商品データ取得
  - `shopify_graphql(query)`: カスタムGraphQLクエリ
- `ga4_client.py`: GA4 Data APIクライアント（サービスアカウント認証）
  - `run_report(dimensions, metrics, start_date, end_date)`: 汎用レポート実行（DataFrameで返却）
  - `get_traffic_overview(start_date, end_date)`: チャネル別セッション・ユーザー数
  - `get_daily_traffic(start_date, end_date)`: 日別トラフィック推移（セッション・売上等）
  - `get_source_medium(start_date, end_date)`: 参照元/メディア別トラフィック
  - `get_landing_pages(start_date, end_date)`: ランディングページ別パフォーマンス
  - `get_device_breakdown(start_date, end_date)`: デバイス別トラフィック
  - `get_geo_data(start_date, end_date)`: 地域（国・都市）別トラフィック
  - `get_page_views(start_date, end_date)`: ページ別PV・滞在時間
- pandas, matplotlib: データ分析・可視化

## 分析時の行動原則
1. データを取得したら、まず全体像を見せる（件数、期間、主要指標）
2. 数字を出すだけでなく「So What?（だから何？）」を必ず添える
3. 分析後は必ず次の探索方向を2〜3個提案する
4. 指示が曖昧なときは確認してから動く
5. グラフを作るときは日本語ラベルを使用する

## 出力形式
- 分析結果は output/ ディレクトリに保存
- グラフはPNG形式で output/ に保存
- レポートはMarkdown形式で output/ に保存