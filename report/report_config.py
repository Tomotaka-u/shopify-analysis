"""
キャンペーン定義と比較プリセット
新しいセール比較を追加する場合はここにキャンペーンと比較プリセットを追加するだけ。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class Campaign:
    id: str                 # 一意のID (例: "bf2025")
    name: str               # 正式名称 (例: "BF 2025")
    short_name: str         # 短縮名 (例: "BF")
    start_date: date
    end_date: date
    data_dir: str           # data/ 配下のディレクトリ名
    css_class: str          # テンプレートで使うCSS識別子 ("bf" or "ny" 等)
    color_var: str           # CSS変数名 (例: "coral", "blue")

    @property
    def days(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def period_label(self) -> str:
        s = self.start_date
        e = self.end_date
        return f"{s.month}/{s.day} ~ {e.month}/{e.day}（{self.days}日間）"

    @property
    def ga4_start(self) -> str:
        return self.start_date.isoformat()

    @property
    def ga4_end(self) -> str:
        return self.end_date.isoformat()


@dataclass
class ComparisonConfig:
    """2つのキャンペーンの比較設定"""
    campaign_a: Campaign
    campaign_b: Campaign
    title: str                          # レポートタイトル
    orders_csv: str                     # 全期間 orders.csv のパス
    all_social_csv: str | None = None   # 全期間ソーシャルリファラーCSV
    sidebar_title: str = ""             # サイドバー表示名
    insights: dict = field(default_factory=dict)  # セクション別インサイトテキスト (オプション)

    def __post_init__(self):
        if not self.sidebar_title:
            self.sidebar_title = (
                f"{self.campaign_a.short_name} vs {self.campaign_b.short_name}"
            )


# ── キャンペーン定義 ──

BF2025 = Campaign(
    id="bf2025",
    name="BF 2025",
    short_name="BF",
    start_date=date(2025, 11, 15),
    end_date=date(2025, 12, 6),
    data_dir="bf2025",
    css_class="bf",
    color_var="coral",
)

NEWYEAR2025 = Campaign(
    id="newyear2025",
    name="新春 2025",
    short_name="新春",
    start_date=date(2025, 12, 26),
    end_date=date(2026, 1, 6),
    data_dir="newyear2025",
    css_class="ny",
    color_var="blue",
)


# ── 比較プリセット ──

BF_VS_NEWYEAR = ComparisonConfig(
    campaign_a=BF2025,
    campaign_b=NEWYEAR2025,
    title="BF 2025 vs 新春セール 2025",
    orders_csv="data/all/orders.csv",
    all_social_csv="data/all/sales_by_social_referrer.csv",
    sidebar_title="BF vs 新春",
    insights={
        "summary": [
            {
                "num": 1,
                "text": "<strong>BFの強さの源泉はInstagramのバイラル効果。</strong>"
                        "Organic Social（主にIG）がBF全体の36.6%を占め、アプリ内ブラウザ経由で2,230セッション流入。"
                        "新春ではこれが26に激減し、売上差の最大要因となった。",
            },
            {
                "num": 2,
                "text": "<strong>CVRの差はわずか0.3pt。差は集客力にある。</strong>"
                        "BF 5.1% vs 新春 4.8%。ファネル転換効率はほぼ同じで、"
                        "セッション数の差（10,392 vs 3,725）が売上差に直結。",
            },
            {
                "num": 3,
                "text": "<strong>YouTubeのCVRが全チャネル最強（推定15.1%）。</strong>"
                        "セッション数はIGの1/6だが、Shopify帰属売上ではYouTube &yen;485,752 > Instagram &yen;42,732。"
                        "動画で商品理解してから来るため購買率が高い。",
            },
            {
                "num": 4,
                "text": "<strong>新春はアフィリエイター依存が高い。</strong>"
                        "Affiliate比率がBF 11.6% → 新春 24.1%。"
                        "Tina + ni5.chanの2名が最終日1/6の注文の51%を占める。",
            },
            {
                "num": 5,
                "text": "<strong>最終日のCVRが突出する。</strong>"
                        "BF 12/6のCVR 13.0%は通常の3倍。"
                        "「今日で終了」の緊急性が最大のCVRブースター。",
            },
        ],
        "sales_discount": "割引適用率 = 割引コード経由の注文比率。"
                          "BFはコードなし22.4%が多く、SNSで見て直接購入する層が厚い。"
                          "新春は82.2%がコード経由で、アフィリエイター依存度が高い。",
        "cvr_funnel": "ファネル形状はほぼ同じ。カート追加→チェックアウト到達の維持率は両方とも約84%。"
                      "チェックアウト到達→購入完了はBF 55.8%、新春 58.5%で、新春の方がやや「迷わず買う」傾向。"
                      "カート→購入率もBF 47.1% vs 新春 49.4%で新春が上。",
        "cvr_daily": "BF最終日12/6のCVR 13.0%は期間中最高。"
                     "セッション710（ピーク11/28の半分）でCVR約2倍 = "
                     "「本当に買いたい人だけが来ている」状態。最終日の緊急性が最大のCVRブースター。",
        "traffic_channel": "BFはOrganic Social（36.6%）が最大。新春はAffiliate比率が倍増（11.6%→24.1%）し、"
                           "Direct（30.2%）が最大チャネルに。SNSバズの有無が集客構造を根本的に変えている。",
        "traffic_ig": "ig（アプリ内ブラウザ）が2,230→26に壊滅。ただし新春のインフルエンサーは"
                      "アフィリエイトリンク（leaddyno）を使用しているため、実際にはIGからの流入が"
                      "leaddyno（943セッション）に計上されている。"
                      "BFのig 2,230はアフィリエイトリンクを使わない直接リンク or フォロワーの二次拡散（リポスト等）と推定。",
        "social_sales": "全期間累計でYouTube（&yen;305万）がInstagram（&yen;242万）を26%上回る。"
                        "セッション数はIGの方が圧倒的に多いにも関わらず。"
                        "ただしIG経由のアフィリエイトリンク売上はAffiliate枠に帰属されるため、"
                        "IGの実際の貢献度は過小評価されている点に注意。",
        "social_cvr": "YouTube BF推定CVR 15.1% = 6-7人に1人が購入。"
                      "動画で商品を理解した上でサイトに来るため購買意欲が高い。"
                      "エンゲージメントもYouTubeが最高（滞在166-241秒、PV/ss 5.5-5.8）で、IGの2-3倍。",
        "products": "BFはimport完全版が78.7%を占める一本勝負。新春では61.6%に低下し、他商品に分散。"
                    "新春の方が「他の商品も見て選ぶ」行動が多い。",
        "affiliates": "Tinaは2024年7-8月に161件を記録後、休眠。BFは1件のみだったが、新春で35件と復活。"
                      "12/27から徐々に再開し、最終日1/6に16件を集中投下した意図的な復帰パターン。",
        "peak_channel": "BFピークはIG 44.1%が主導、新春ピークはAffiliate 44.8%が主導。構成比がほぼ鏡像。"
                        "BFは「SNSでバズって自発的に買いに来る」、新春は「アフィリエイターの告知リンクを踏んで来る」"
                        "と構造が全く異なる。",
        "time_block": "全注文の37.5%が18-23時に集中。セール時はさらに夜偏重（BF 42.8%、新春 50.6%）。"
                      "ゴールデンタイムは21-23時の3時間で全注文の22.4%（1,079件）。",
        "day_of_week": "日曜が最強（18.8%）、木曜が最弱（12.1%）。平均注文額は時間帯・曜日による差がほぼなく"
                       "（&yen;3,900-4,400）、件数の多さだけが変動要因。"
                       "SNS投稿やセール告知の最適タイミングは「日曜 or 土曜の20-21時」。",
        "recommendations": [
            {
                "title": "YouTube経由の集客を強化する",
                "body": "推定CVR 15.1%は全チャネル最強。セッション数はIGの1/6しかないため伸びしろが大きい。"
                        "アフィリエイターにYouTubeレビュー動画の制作を依頼するなど、動画経由の流入拡大を優先。",
            },
            {
                "title": "新春セールのIG施策を強化する",
                "body": "BFでのIG流入3,341 → 新春581の差がセール間の売上差に直結。"
                        "新春でもストーリーズ/リールでの直接リンク投稿を増やし、フォロワーのリポストを促す仕掛けが必要。",
            },
            {
                "title": "最終日効果を意図的に設計する",
                "body": "BF 12/6のCVR 13.0%は通常の3倍。「今日最終日」の告知をアフィリエイター全員に依頼し、"
                        "土日の20-21時に投稿を集中させることで駆け込み購入を最大化。",
            },
            {
                "title": "アフィリエイター依存のリスクを分散する",
                "body": "新春は特定2名（Tina + ni5.chan）で注文の51%。boo!のようにBFのみ稼働するパターンもある。"
                        "常時3-4名が安定稼働する体制と、コードなし（自発的購入）比率の向上が課題。",
            },
            {
                "title": "投稿の最適タイミング",
                "body": "全注文の22.4%が21-23時に集中、曜日は日曜が最強（18.8%）。"
                        "セール告知やアフィリエイター投稿は「日曜 or 土曜の20-21時」が最適。"
                        "22-23時の購入ピークの1-2時間前に投稿して駆け込み購入を最大化する。",
                "full_width": True,
            },
        ],
    },
)

# デフォルト比較
DEFAULT_COMPARISON = BF_VS_NEWYEAR
