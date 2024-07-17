

import json
from tap_decentraland_thegraph.client import DecentralandTheGraphStream
from typing import Optional
from singer_sdk import typing as th  # JSON Schema typing helpers


class CollectionStream(DecentralandTheGraphStream):
    @property
    def partitions(self):
        return [{"path": path} for path in self.config["collection_paths"]]

    def get_url(self, context: Optional[dict]) -> str:
        path = context["path"]
        return super().get_url(context) + f"/{path}"


class MintsStream(CollectionStream):
    name = "collection_mints"
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    primary_keys = ['id']
    is_sorted = True
    records_jsonpath = '$.data.mints[*]'
    next_page_token_jsonpath = '$.data.mints[-1].timestamp'
    query = """
        query($updatedAt: Int!){
        mints(
            first: 1000
            orderBy: timestamp
            orderDirection: asc,
            where: {
                timestamp_gt: $updatedAt
            }
        )
        {
            id,
            beneficiary,
            minter,
            timestamp,
            searchPrimarySalePrice
            searchContractAddress
            searchItemId
            searchTokenId
            searchIssuedId
            searchIsStoreMinter
            nft {
                id
                contractAddress
                tokenId
                itemBlockchainId
            }
        }
    }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['id'] = row['id']
        result['collection_id'] = row['nft']['contractAddress']
        result['item_id'] = row['nft']['contractAddress'] + \
            '-' + row['nft']['itemBlockchainId']
        result['token_id'] = row['nft']['tokenId']
        result['beneficiary'] = row['beneficiary']
        result['minter'] = row['minter']
        result['timestamp'] = str(row['timestamp'])
        result['mint_price'] = row['searchPrimarySalePrice']
        result['contract_address'] = row['searchContractAddress']
        result['issued_id'] = row['searchIssuedId']
        result['is_store_minter'] = row['searchIsStoreMinter']

        path = context["path"]

        result['network'] = 'Ethereum' if path == 'collections-ethereum-mainnet' else 'Matic'

        return result

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("collection_id", th.StringType),
        th.Property("item_id", th.StringType),
        th.Property("token_id", th.StringType),
        th.Property("beneficiary", th.StringType),
        th.Property("minter", th.StringType),
        th.Property("timestamp", th.StringType),
        th.Property("mint_price", th.StringType),
        th.Property("contract_address", th.StringType),
        th.Property("issued_id", th.StringType),
        th.Property("is_store_minter", th.BooleanType),
        th.Property("network", th.StringType)
    ).to_dict()


class OrdersStream(CollectionStream):
    name = "collection_orders"
    replication_key = 'updated_at'
    primary_keys = ['id']
    replication_method = "INCREMENTAL"
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
                    owner,
                    price,
                    txHash
                    buyer,
                    blockNumber,
                    createdAt,
                    updatedAt,
                    expiresAt,
                    status,
                    nft {
                        id
                        tokenId
                        contractAddress
                        itemBlockchainId
                    }
                }
            }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['id'] = row['id']
        result['collection_id'] = row['nft']['contractAddress']
        result['item_id'] = row['nft']['contractAddress'] + \
            '-' + row['nft']['itemBlockchainId']
        result['token_id'] = row['nft']['tokenId']
        result['marketplace_address'] = row['marketplaceAddress']
        result['owner'] = row['owner']
        result['price'] = row['price']
        result['tx_hash'] = row['txHash']
        result['buyer'] = row['buyer']
        result['block_number'] = row['blockNumber']
        result['created_at'] = row['createdAt']
        result['updated_at'] = row['updatedAt']
        result['expires_at'] = row['expiresAt']
        result['status'] = row['status']

        path = context["path"]

        result['network'] = 'Ethereum' if path == 'collections-ethereum-mainnet' else 'Matic'

        return result

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("collection_id", th.StringType),
        th.Property("item_id", th.StringType),
        th.Property("token_id", th.StringType),
        th.Property("marketplace_address", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("price", th.StringType),
        th.Property("tx_hash", th.StringType),
        th.Property("buyer", th.StringType),
        th.Property("block_number", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
        th.Property("expires_at", th.StringType),
        th.Property("status", th.StringType),
        th.Property("network", th.StringType)
    ).to_dict()


class BidsStream(CollectionStream):
    name = "collection_bids"
    replication_key = 'updated_at'
    primary_keys = ['id']
    replication_method = "INCREMENTAL"
    is_sorted = True
    records_jsonpath = '$.data.bids[*]'
    next_page_token_jsonpath = '$.data.bids[-1].updatedAt'
    query = """
            query($updatedAt: Int!){
                bids(
                    first: 1000
                    orderBy: updatedAt
                    orderDirection: asc,
                    where: {
                        updatedAt_gt: $updatedAt
                    }
                )
                {
                    id,
                    bidAddress,
                    nft {
                        id
                        tokenId
                        contractAddress
                        itemBlockchainId
                    }
                    bidder,
                    seller,
                    price,
                    status,
                    blockNumber,
                    createdAt,
                    updatedAt,
                    expiresAt
                }
            }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['id'] = row['id']
        result['collection_id'] = row['nft']['contractAddress']
        result['item_id'] = row['nft']['contractAddress'] + \
            '-' + row['nft']['itemBlockchainId']
        result['token_id'] = row['nft']['tokenId']
        result['bid_address'] = row['bidAddress']
        result['bidder'] = row['bidder']
        result['seller'] = row['seller']
        result['price'] = row['price']
        result['status'] = row['status']
        result['block_number'] = row['blockNumber']
        result['created_at'] = row['createdAt']
        result['updated_at'] = row['updatedAt']
        result['expires_at'] = row['expiresAt']

        path = context["path"]

        result['network'] = 'Ethereum' if path == 'collections-ethereum-mainnet' else 'Matic'

        return result

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("collection_id", th.StringType),
        th.Property("item_id", th.StringType),
        th.Property("token_id", th.StringType),
        th.Property("bid_address", th.StringType),
        th.Property("bidder", th.StringType),
        th.Property("seller", th.StringType),
        th.Property("price", th.StringType),
        th.Property("status", th.StringType),
        th.Property("block_number", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
        th.Property("expires_at", th.StringType),
        th.Property("network", th.StringType)
    ).to_dict()


class CollectionsStream(CollectionStream):
    name = "collections"
    replication_key = 'updated_at'
    primary_keys = ['collection_id']
    replication_method = "INCREMENTAL"
    is_sorted = True
    records_jsonpath = '$.data.collections[*]'
    next_page_token_jsonpath = '$.data.collections[-1].updatedAt'
    query = """
        query($updatedAt: Int!){
            collections (
                first: 1000
                orderBy: updatedAt
                orderDirection: asc,
                where: {
                        updatedAt_gt: $updatedAt
                }
            )
            {
                id,
                owner,
                creator,
                name,
                symbol,
                isCompleted,
                isApproved,
                isEditable,
                minters,
                managers,
                urn,
                itemsCount,
                createdAt,
                updatedAt,
                reviewedAt,
                firstListedAt,
                searchIsStoreMinter,
                searchText
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['collection_id'] = row['id']
        result['owner'] = row['owner']
        result['creator'] = row['creator']
        result['name'] = row['name']
        result['symbol'] = row['symbol']
        result['is_completed'] = row['isCompleted']
        result['is_approved'] = row['isApproved']
        result['is_editable'] = row['isEditable']
        result['managers'] = json.dumps(row['managers'])
        result['urn'] = row['urn']
        result['items_count'] = row['itemsCount']
        result['created_at'] = row['createdAt']
        result['updated_at'] = row['updatedAt']
        result['reviewed_at'] = row['reviewedAt']
        result['first_listed_at'] = row['firstListedAt']
        result['is_store_minter'] = row['searchIsStoreMinter']
        result['search_text'] = row['searchText']

        path = context["path"]

        result['network'] = 'Ethereum' if path == 'collections-ethereum-mainnet' else 'Matic'

        return result

    schema = th.PropertiesList(
        th.Property("collection_id", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("creator", th.StringType),
        th.Property("name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("is_completed", th.BooleanType),
        th.Property("is_approved", th.BooleanType),
        th.Property("is_editable", th.BooleanType),
        th.Property("managers", th.StringType),
        th.Property("urn", th.StringType),
        th.Property("items_count", th.IntegerType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
        th.Property("reviewed_at", th.StringType),
        th.Property("first_listed_at", th.StringType),
        th.Property("is_store_minter", th.BooleanType),
        th.Property("search_text", th.StringType),
        th.Property("network", th.StringType)
    ).to_dict()


class ItemsStream(CollectionStream):
    name = "collection_items"
    replication_key = 'updated_at'
    replication_method = "INCREMENTAL"
    is_sorted = True
    records_jsonpath = '$.data.items[*]'
    next_page_token_jsonpath = '$.data.items[-1].updatedAt'
    query = """
    query($updatedAt: Int!){
        items(
            first: 10
            orderBy: updatedAt
            orderDirection: asc,
            where: {
                updatedAt_gt: $updatedAt
            }
        )
        {
            id,
            itemType,
            totalSupply,
            maxSupply,
            rarity,
            creationFee,
            available,
            price,
            contentHash,
            URI,
            image,
            metadata {
                wearable {
                    name,
                    description,
                    category,
                    rarity,
                    bodyShapes
                },
                emote {
                    name,
                    description,
                    category,
                    loop,
                    rarity,
                    bodyShapes,
                    hasSound,
                    hasGeometry
                }
            },
            urn,
            reviewedAt,
            soldAt,
            firstListedAt,
            sales,
            volume,
            createdAt,
            updatedAt
        }
    }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['id'] = row['id']
        result['created_at'] = row['createdAt']
        result['updated_at'] = row['updatedAt']
        result['item_type'] = row['itemType']
        result['total_supply'] = row['totalSupply']
        result['max_supply'] = row['maxSupply']
        result['rarity'] = row['rarity']
        result['creation_fee'] = row['creationFee']
        result['available'] = row['available']
        result['price'] = row['price']
        result['uri'] = row['URI']
        result['image'] = row['image']
        result['metadata'] = json.dumps(row['metadata'])
        result['urn'] = row['urn']
        result['reviewed_at'] = row['reviewedAt']
        result['sold_at'] = row['soldAt']
        result['first_listed_at'] = row['firstListedAt']
        result['sales'] = row['sales']
        result['volume'] = row['volume']

        path = context["path"]

        result['network'] = 'Ethereum' if path == 'collections-ethereum-mainnet' else 'Matic'

        return result

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
        th.Property("item_type", th.StringType),
        th.Property("total_supply", th.StringType),
        th.Property("max_supply", th.StringType),
        th.Property("rarity", th.StringType),
        th.Property("creation_fee", th.StringType),
        th.Property("available", th.StringType),
        th.Property("price", th.StringType),
        th.Property("uri", th.StringType),
        th.Property("image", th.StringType),
        th.Property("metadata", th.StringType),
        th.Property("urn", th.StringType),
        th.Property("reviewed_at", th.StringType),
        th.Property("sold_at", th.StringType),
        th.Property("first_listed_at", th.StringType),
        th.Property("sales", th.IntegerType),
        th.Property("volume", th.StringType),
        th.Property("network", th.StringType)
    ).to_dict()
