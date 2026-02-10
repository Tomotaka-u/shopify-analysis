"""
Shopify API クライアント
- Client Credentials方式でトークンを自動取得・リフレッシュ
- Claude Codeから呼び出して使う
"""

import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

STORE = os.getenv("SHOPIFY_STORE")
CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")

# トークンキャッシュ
_token_cache = {"token": None, "expires_at": None}


def get_access_token():
    """アクセストークンを取得（24時間有効、期限切れなら自動更新）"""
    if _token_cache["token"] and _token_cache["expires_at"] > datetime.now():
        return _token_cache["token"]

    response = requests.post(
        f"https://{STORE}/admin/oauth/access_token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
    )
    response.raise_for_status()
    data = response.json()

    _token_cache["token"] = data["access_token"]
    _token_cache["expires_at"] = datetime.now() + timedelta(seconds=data["expires_in"] - 60)

    return data["access_token"]


def shopify_graphql(query, variables=None):
    """Shopify GraphQL Admin APIにクエリを送る"""
    token = get_access_token()
    response = requests.post(
        f"https://{STORE}/admin/api/2025-01/graphql.json",
        json={"query": query, "variables": variables or {}},
        headers={
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()
    return response.json()


# ===== よく使うクエリ =====

def get_orders(start_date, end_date, limit=250):
    """指定期間の注文データを全件取得（自動ページネーション）

    Returns:
        dict: {"data": {"orders": {"edges": [...全ノード...]}}} 形式
              （既存コードと互換性のあるレスポンス形式）
    """
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
                    subtotalPriceSet { shopMoney { amount currencyCode } }
                    totalDiscountsSet { shopMoney { amount currencyCode } }
                    displayFinancialStatus
                    displayFulfillmentStatus
                    customer {
                        id
                        email
                        numberOfOrders
                        amountSpent { amount currencyCode }
                        createdAt
                    }
                    lineItems(first: 50) {
                        edges {
                            node {
                                title
                                quantity
                                originalUnitPriceSet { shopMoney { amount currencyCode } }
                                discountedUnitPriceSet { shopMoney { amount currencyCode } }
                            }
                        }
                    }
                    discountCodes
                    referrerUrl
                    tags
                }
            }
            pageInfo {
                hasNextPage
            }
        }
    }
    """
    all_edges = []
    cursor = None

    while True:
        variables = {
            "query": f"created_at:>={start_date} created_at:<={end_date}",
            "first": limit,
            "after": cursor,
        }
        result = shopify_graphql(query, variables)

        if "errors" in result:
            return result

        edges = result["data"]["orders"]["edges"]
        all_edges.extend(edges)

        if not result["data"]["orders"]["pageInfo"]["hasNextPage"]:
            break

        cursor = edges[-1]["cursor"]

    return {"data": {"orders": {"edges": all_edges}}}


def get_customers(limit=250):
    """顧客データを取得"""
    query = """
    query($first: Int!) {
        customers(first: $first) {
            edges {
                node {
                    id
                    email
                    firstName
                    lastName
                    ordersCount
                    totalSpentV2 { amount currencyCode }
                    createdAt
                    updatedAt
                    tags
                    defaultAddress {
                        country
                        province
                        city
                    }
                }
            }
        }
    }
    """
    return shopify_graphql(query, {"first": limit})


def get_products():
    """全商品データを取得"""
    query = """
    {
        products(first: 50) {
            edges {
                node {
                    id
                    title
                    handle
                    status
                    productType
                    createdAt
                    totalInventory
                    variants(first: 50) {
                        edges {
                            node {
                                title
                                price
                                sku
                            }
                        }
                    }
                }
            }
        }
    }
    """
    return shopify_graphql(query)


if __name__ == "__main__":
    # 動作確認用
    print("=== 商品データ取得テスト ===")
    products = get_products()
    print(json.dumps(products, indent=2, ensure_ascii=False))