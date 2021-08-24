"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphPolygonStream


class WearablesOrdersPolygonStream(DecentralandTheGraphPolygonStream):
    name = "orders_polygon_wearables"

    primary_keys = ["id"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'orders'
    
    query = """
    query ($updatedAt: Int!)
        {
            orders(
                first: 1000,
                orderBy: updatedAt,
                orderDirection: asc,
                where:{
                    status:sold,
                    updatedAt_gte: $updatedAt
                }
            )
            {
                id
                owner
                price
                txHash
                buyer
                blockNumber
                updatedAt
                nft {
                    id
                    tokenId
                    contractAddress
                    metadata{
                        wearable {
                            id
                            name
                            collection
                            rarity
                            description
                            bodyShapes
                        }
                    }
                }
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        bodyShapes = row['nft']['metadata']['wearable']['bodyShapes']
        row['nft']['metadata']['wearable']['bodyShapeMale'] = 'BaseMale' in bodyShapes
        row['nft']['metadata']['wearable']['bodyShapeFemale'] = 'BaseFemale' in bodyShapes
        del row['nft']['metadata']['wearable']['bodyShapes']
        return row

    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("owner", th.StringType),
        th.Property("price", th.StringType),
        th.Property("txHash", th.StringType),
        th.Property("buyer", th.StringType),
        th.Property("blockNumber", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("nft", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("tokenId", th.StringType),
            th.Property("contractAddress", th.StringType),
            th.Property("metadata", th.ObjectType(
                th.Property("wearable", th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("collection", th.StringType),
                    th.Property("rarity", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property("bodyShapeMale", th.BooleanType),
                    th.Property("bodyShapeFemale", th.BooleanType),
                )),
            ))
        )),
    ).to_dict()


class WearablesPrimarySalesPolygonStream(DecentralandTheGraphPolygonStream):
    name = "primary_sales_polygon_wearables"

    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'mints'
    
    query = """
    query ($timestamp: Int!)
        {
        mints(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where:{
                timestamp_gte: $timestamp
            }
        ) {
            id
            beneficiary
            minter
            timestamp
            searchPrimarySalePrice
            searchContractAddress
            searchItemId
            searchTokenId
            searchIssuedId
            searchIsStoreMinter
        }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row['searchIssuedId'] = int(row['searchIssuedId'])
        return row

    
    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or self.get_starting_timestamp(partition)
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "timestamp": int(next_page_token),
        }
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("beneficiary", th.StringType),
        th.Property("minter", th.StringType),
        th.Property("timestamp", th.StringType),
        th.Property("searchPrimarySalePrice", th.StringType),
        th.Property("searchContractAddress", th.StringType),
        th.Property("searchItemId", th.StringType),
        th.Property("searchTokenId", th.StringType),
        th.Property("searchIssuedId", th.IntegerType),
        th.Property("searchIsStoreMinter", th.BooleanType)
    ).to_dict()