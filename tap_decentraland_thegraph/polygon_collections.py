"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphPolygonStream


class MintsPolygonStream(DecentralandTheGraphPolygonStream):
    name = "nfts_mints_polygon"
    primary_keys = ["rowId"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'mints'

    query = """
    query ($updatedAt: Int!)
    {
        mints (
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where:{
                timestamp_gte: $updatedAt
            }
        )
        {
            id
            item{
                id
                creator
                itemType
                available
                totalSupply
                maxSupply
                rarity
                creationFee
                image
                createdAt
                reviewedAt
                searchIsCollectionApproved
            }
            creator
            beneficiary
            minter
            timestamp
        }
    }


    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['rowId'] = "|".join([row['id'], row['timestamp']])
        return row

    schema = th.PropertiesList(
        th.Property("rowId", th.StringType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("creator", th.StringType),
        th.Property("beneficiary", th.StringType),
        th.Property("minter", th.StringType),
        th.Property("timestamp", th.StringType),
        th.Property("item", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("creator", th.StringType),
            th.Property("itemType", th.StringType),
            th.Property("available", th.StringType),
            th.Property("totalSupply", th.StringType),
            th.Property("maxSupply", th.StringType),
            th.Property("rarity", th.StringType),
            th.Property("creationFee", th.StringType),
            th.Property("image", th.StringType),
            th.Property("createdAt", th.StringType),
            th.Property("reviewedAt", th.StringType),
            th.Property("searchIsCollectionApproved", th.BooleanType),
        )),
    ).to_dict()


class MintsPolygonStreamV2(DecentralandTheGraphPolygonStream):
    name = "nfts_mints_polygon_v2"

    primary_keys = ["nft_id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'mints'

    query = """
    query ($updatedAt: Int!)
    {
        mints(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where:{
                timestamp_gte: $updatedAt
            }
        )
        {
            id
            item{
                id
            }
            creator
            beneficiary
            minter
            timestamp
            searchPrimarySalePrice
            searchContractAddress
            searchTokenId
            searchIssuedId
            searchIsStoreMinter
        }
    }


    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        result = {}

        result['nft_id'] = row['item']['id'] + '-' + row['searchTokenId']
        result['seller_address'] = row['creator']
        result['buyer_address'] = row['beneficiary']
        result['minter_address'] = row['minter']
        result['timestamp'] = row['timestamp']
        result['primary_sale_price'] = row['searchPrimarySalePrice']
        result['collection_id'] = row['searchContractAddress']
        result['item_id'] = row['item']['id']
        result['is_store_minter'] = row['searchIsStoreMinter']

        return result

    schema = th.PropertiesList(
        th.Property("nft_id", th.StringType, required=True),
        th.Property("seller_address", th.StringType),
        th.Property("buyer_address", th.StringType),
        th.Property("minter_address", th.StringType),
        th.Property("timestamp", th.StringType),
        th.Property("primary_sale_price", th.StringType),
        th.Property("collection_id", th.StringType),
        th.Property("item_id", th.StringType),
        th.Property("is_store_minter", th.BooleanType),
    ).to_dict()


class CurationsPolygonStream(DecentralandTheGraphPolygonStream):
    name = "nfts_curations_polygon"

    primary_keys = ["tx_hash"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'curations'

    query = """
    query ($updatedAt: Int!)
    {
        curations(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where:{
                timestamp_gte: $updatedAt
            }
        )
        {
            id
            txHash
            curator {
                address
            }
            collection {
                id
            }
            isApproved
            timestamp
        }
    }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:

        result = {}

        result['curation_id'] = row['id']
        result['tx_hash'] = row['txHash']
        result['curator_address'] = row['curator']['address']
        result['collection_id'] = row['collection']['id']
        result['is_approved'] = row['isApproved']
        result['timestamp'] = row['timestamp']

        return result

    schema = th.PropertiesList(
        th.Property("curation_id", th.StringType, required=True),
        th.Property("tx_hash", th.StringType),
        th.Property("curator_address", th.StringType),
        th.Property("collection_id", th.StringType),
        th.Property("is_approved", th.BooleanType),
        th.Property("timestamp", th.StringType),
    ).to_dict()
