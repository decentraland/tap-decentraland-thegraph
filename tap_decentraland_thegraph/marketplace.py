

from typing import Optional
from tap_decentraland_thegraph.client import DecentralandTheGraphStream
from singer_sdk import typing as th  # JSON Schema typing helpers


class MarketplaceStream(DecentralandTheGraphStream):
    def get_url(self, context: Optional[dict]) -> str:
        return super().get_url(context) + "/marketplace"


class MarketplaceOrdersStream(MarketplaceStream):
    name = "marketplace_orders"
    replication_key = 'updated_at'
    replication_method = "INCREMENTAL"
    primary_keys = ['id']
    is_sorted = True
    records_jsonpath = '$.data.orders[*]'
    next_page_token_jsonpath = '$.data.orders[-1].updatedAt'
    query = """
        query($updatedAt: Int!){
            orders(
                first: 1000
                orderBy: updatedAt
                orderDirection: asc,
                where: {
                    updatedAt_gt: $updatedAt
                }
            )
            {
                id,
                marketplaceAddress,
                category,
                nft {
                    id,
                    contractAddress,
                    tokenId,
                }
                txHash,
                owner,
                buyer,
                price,
                status,
                blockNumber,
                expiresAt,
                createdAt,
                updatedAt,
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['id'] = row['id']
        result['marketplace_address'] = row['marketplaceAddress']
        result['category'] = row['category']
        result['nft_id'] = row['nft']['id']
        result['contract_address'] = row['nft']['contractAddress']
        result['token_id'] = row['nft']['tokenId']
        result['tx_hash'] = row['txHash']
        result['owner'] = row['owner']
        result['buyer'] = row['buyer']
        result['price'] = row['price']
        result['status'] = row['status']
        result['block_number'] = row['blockNumber']
        result['expires_at'] = row['expiresAt']
        result['created_at'] = row['createdAt']
        result['updated_at'] = row['updatedAt']

        return result

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("marketplace_address", th.StringType),
        th.Property("category", th.StringType),
        th.Property("nft_id", th.StringType),
        th.Property("contract_address", th.StringType),
        th.Property("token_id", th.StringType),
        th.Property("tx_hash", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("buyer", th.StringType),
        th.Property("price", th.StringType),
        th.Property("status", th.StringType),
        th.Property("block_number", th.StringType),
        th.Property("expires_at", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()


class MarketplaceBidsStream(MarketplaceStream):
    name = "marketplace_bids"
    replication_key = 'updated_at'
    replication_method = "INCREMENTAL"
    primary_keys = ['id']
    is_sorted = True
    records_jsonpath = '$.data.bids[*]'
    next_page_token_jsonpath = '$.data.orders[-1].updatedAt'
    query = """
        query($updatedAt: Int!){
            bids(
                first: 1000
                orderBy: updatedAt
                orderDirection: asc,
                where: {
                    updatedAt_gte: $updatedAt
                }
            )
            {
                id,
                bidAddress,
                category,
                nft {
                    id,
                    contractAddress,
                    tokenId,   
                }
                bidder,
                seller,
                price,
                fingerprint,
                status,
                blockchainId,
                blockNumber,
                expiresAt,
                createdAt,
                updatedAt
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['id'] = row['id']
        result['bid_address'] = row['bidAddress']
        result['category'] = row['category']
        result['nft_id'] = row['nft']['id']
        result['contract_address'] = row['nft']['contractAddress']
        result['token_id'] = row['nft']['tokenId']
        result['bidder'] = row['bidder']
        result['seller'] = row['seller']
        result['price'] = row['price']
        result['fingerprint'] = row['fingerprint']
        result['status'] = row['status']
        result['blockchain_id'] = row['blockchainId']
        result['block_number'] = row['blockNumber']
        result['expires_at'] = row['expiresAt']
        result['created_at'] = row['createdAt']
        result['updated_at'] = row['updatedAt']

        return result

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("bid_address", th.StringType),
        th.Property("category", th.StringType),
        th.Property("nft_id", th.StringType),
        th.Property("contract_address", th.StringType),
        th.Property("token_id", th.StringType),
        th.Property("bidder", th.StringType),
        th.Property("seller", th.StringType),
        th.Property("price", th.StringType),
        th.Property("fingerprint", th.StringType),
        th.Property("status", th.StringType),
        th.Property("blockchain_id", th.StringType),
        th.Property("block_number", th.StringType),
        th.Property("expires_at", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()
