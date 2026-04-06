# 週次比較レポート生成

対象期間: $ARGUMENTS (例: 3/29-4/4)

引数がない場合は、前回の期間（`analysis_weekly_comparison.py` の THIS_WEEK 定数）を確認し、その翌週を対象にする。

以下の手順を順番に実行すること。

---

## Step 1: スクリプトの期間更新 & 実行 【Sonnetで実行】

1. `analysis_weekly_comparison.py` を開き、以下を更新:
   - `THIS_WEEK_START` / `THIS_WEEK_END` → 今週の期間
   - `LAST_WEEK_START` / `LAST_WEEK_END` → 先週の期間
   - 出力ファイル名の `weekly_report_YYYYMMDD` / `weekly_data_YYYYMMDD` → 今週の終了日

2. 実行:
```bash
python3 analysis_weekly_comparison.py
```

3. 以下が生成されたことを確認:
   - `output/weekly/weekly_report_YYYYMMDD.md`
   - `output/weekly/weekly_data_YYYYMMDD.json`

---

## Step 2a: インサイト執筆 【Opus Agentに委譲】

Agent tool で `model: "opus"` を指定し、以下のプロンプトで依頼する。

```
週次レポートのインサイト執筆を依頼します。

## 入力データ
以下のJSONファイルを読んでください:
- `output/weekly/weekly_data_YYYYMMDD.json`

## 参考テンプレート
過去HTMLのインサイト部分を参考にしてください:
- `output/weekly/weekly_comparison_20260404.html`

## 書くべき項目（全10項目）
1. 主要ファインディング（4点、箇条書き。HTMLの summary-points 用）
2. 日別売上の傾向コメント（1段落）
3. 商品別の分析コメント（1段落）
4. アフィリエイト分析コメント（1段落）
5. トラフィック概要コメント（1段落）
6. デバイス別コメント（1段落）
7. CVRファネルコメン��（1段落）
8. チャネル���コメント（1段落）
9. 総合分析の全体像（1段落、p タグ用）
10. 次週への提言（4枚のカード用、各タイトル+本文）

## ルール
- CVRは `Shopify注文数 ÷ GA4セッション数` で算出（GA4の transactions は常に0）
- 数値は必ずJSONから正確に引用。計算ミス厳禁
- 各項目に「だから何？（So What?）」を必ず添える
- 日本語で、HTMLタグなしのプレーンテキストで出力
- 各項目を番号付きで明確に区切って出力
```

---

## Step 2b: HTML組み立て 【Sonnetで実行】

1. テンプレート参照: `output/weekly/weekly_comparison_20260404.html`
2. テンプレートのHTML構造（CSS・JS含む）をそのまま流用
3. 以下を差し替え:
   - ヘッダーの期間表示・日付
   - サイドバーのメタ情報（作成日・先週・今週の期間）
   - KPIカードの数値（売上・注文数・AOV・CVR）
   - 日別売上テーブル（先週・今週���
   - 商品別テーブル
   - アフィリエイトテ��ブル
   - トラフィック概要テーブル
   - デバイス別テーブル
   - ファネルの数値
   - チャネル別バーチャートの数値・幅（最大チャネルを100%として比率計算）
   - 参照元トップ10テーブル
   - 全インサイト文 → Step 2a の Opus 出力で置換
4. `output/weekly/weekly_comparison_YYYYMMDD.html` として保存

---

## Step 3: Netlifyデプロイ 【Sonnetで実行】

```bash
# 1. デプロイ用ディレクトリ準備
mkdir -p deploy
cp output/weekly/weekly_comparison_YYYYMMDD.html deploy/index.html

# 2. サイト作成
/opt/homebrew/bin/netlify sites:create --name shopify-weekly-report-YYYYMMDD --account-slug mulen-1002

# 3. リンク切り替え
/opt/homebrew/bin/netlify unlink
/opt/homebrew/bin/netlify link --name shopify-weekly-report-YYYYMMDD

# 4. デプ���イ
/opt/homebrew/bin/netlify deploy --dir=deploy --prod --message="週次レポート M/D-M/D"

# 5. 片付け
rm -rf deploy
```

サイト命名規則: `shopify-weekly-report-YYYYMMDD`（期間終了日）

---

## 完了報告

以下をユーザーに報告:
- デプロイURL: `https://shopify-weekly-report-YYYYMMDD.netlify.app`
- 今週のハイライト（売上・注文数・CVR・注目ポイント）を5行以内で
