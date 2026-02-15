#!/usr/bin/env python3
"""
キャンペーン比較レポート生成CLI

Usage:
  python report/generate_report.py                    # BF vs 新春（GA4あり）
  python report/generate_report.py --no-ga4           # GA4 APIなしで生成
  python report/generate_report.py --output out.html  # 出力パス指定
"""

import argparse
import os
import sys

# プロジェクトルートをパスに追加（ga4_client等のインポート用）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from jinja2 import Environment, FileSystemLoader

from report_config import DEFAULT_COMPARISON
from report_data import load_report_data


def fmt_yen(value) -> str:
    """円表記フォーマット: 1234567 → '1,234,567'"""
    try:
        return f"{int(round(value)):,}"
    except (ValueError, TypeError):
        return str(value)


def fmt_pct(value) -> str:
    """パーセント表記: 0.051 → '5.1%'"""
    try:
        return f"{value * 100:.1f}%"
    except (ValueError, TypeError):
        return str(value)


def fmt_num(value) -> str:
    """数値カンマ区切り: 10392 → '10,392'"""
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


def main():
    parser = argparse.ArgumentParser(description="キャンペーン比較HTMLレポート生成")
    parser.add_argument(
        "--no-ga4",
        action="store_true",
        help="GA4 APIを使わずCSVのみでレポート生成",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="出力HTMLファイルパス（デフォルト: output/<title>.html）",
    )
    args = parser.parse_args()

    config = DEFAULT_COMPARISON
    use_ga4 = not args.no_ga4

    # データ取得
    print(f"データを読み込み中... (GA4: {'ON' if use_ga4 else 'OFF'})")
    ctx = load_report_data(config, use_ga4=use_ga4)

    # Jinja2 テンプレート読み込み
    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
    )
    env.filters["fmt_yen"] = fmt_yen
    env.filters["fmt_pct"] = fmt_pct
    env.filters["fmt_num"] = fmt_num

    template = env.get_template("campaign_comparison.html")

    # レンダリング
    html = template.render(**ctx)

    # 出力
    if args.output:
        output_path = args.output
    else:
        output_dir = os.path.join(_PROJECT_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)
        safe_title = config.campaign_a.id + "_vs_" + config.campaign_b.id
        output_path = os.path.join(output_dir, f"{safe_title}_dashboard.html")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"レポート生成完了: {output_path}")


if __name__ == "__main__":
    main()
