"""Stream type classes for tap-decentraland-thegraph."""

import requests, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphStream


class ETHSalesStream(DecentralandTheGraphStream):
    name = "sales_ethereum"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_collections_url"]

    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'sales'
    
    query = """
    query ($timestamp: Int!)
    {
        sales(
            first: 1000,
            orderBy:timestamp,
            orderDirection:asc
            where:{
                timestamp_gte: $timestamp
                }
        ) {
            id
            type
            buyer
            seller
            price
            feesCollectorCut
            feesCollector
            royaltiesCut
            royaltiesCollector
            item
            {
                id
                blockchainId
                collection {
                    id
                }
                itemType
            }
            nft
            {
                id
                tokenId
                contractAddress
                itemBlockchainId
            }
            timestamp
            txHash
        }
    }
    """

    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or self.get_starting_timestamp(partition)
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "timestamp": int(next_page_token)
        }

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("type", th.StringType),
        th.Property("buyer", th.StringType),
        th.Property("seller", th.StringType),
        th.Property("price", th.StringType),
        th.Property("feesCollectorCut", th.StringType),
        th.Property("feesCollector", th.StringType),
        th.Property("royaltiesCut", th.StringType),
        th.Property("royaltiesCollector", th.StringType),
        th.Property("item", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("blockchainId", th.StringType),
            th.Property("collection", th.ObjectType(
                th.Property("id",th.StringType)
            )),
            th.Property("itemType", th.StringType)
        )),
        th.Property("nft", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("tokenId", th.StringType),
            th.Property("contractAddress", th.StringType),
            th.Property("itemBlockchainId", th.StringType)
        )),
        th.Property("timestamp", th.StringType),
        th.Property("txHash", th.StringType)
    ).to_dict()
    

class PolygonSalesStream(DecentralandTheGraphStream):
    name = "sales_polygon"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["polygon_collections_url"]

    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'sales'
    
    query = """
    query ($timestamp: Int!)
    {
        sales(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc
            where:{
                timestamp_gte: $timestamp                
                }
        ) {
            id
            type
            buyer
            seller
            price
            feesCollectorCut
            feesCollector
            royaltiesCut
            royaltiesCollector
            item
            {
                id
                blockchainId
                collection {
                    id
                }
                itemType
            }
            nft
            {
                id
                tokenId
                contractAddress
                itemBlockchainId
            }
            timestamp
            txHash
        }
    }
    """

    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or self.get_starting_timestamp(partition)
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "timestamp": int(next_page_token)
        }

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("type", th.StringType),
        th.Property("buyer", th.StringType),
        th.Property("seller", th.StringType),
        th.Property("price", th.StringType),
        th.Property("feesCollectorCut", th.StringType),
        th.Property("feesCollector", th.StringType),
        th.Property("royaltiesCut", th.StringType),
        th.Property("royaltiesCollector", th.StringType),
        th.Property("item", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("blockchainId", th.StringType),
            th.Property("collection", th.ObjectType(
                th.Property("id",th.StringType)
            )),
            th.Property("itemType", th.StringType)
        )),
        th.Property("nft", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("tokenId", th.StringType),
            th.Property("contractAddress", th.StringType),
            th.Property("itemBlockchainId", th.StringType)
        )),
        th.Property("timestamp", th.StringType),
        th.Property("txHash", th.StringType)
    ).to_dict()