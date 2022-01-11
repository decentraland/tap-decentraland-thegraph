"""Stream type classes for tap-decentraland-thegraph."""

import requests, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphCompleteObjectStream


class ETHAccountsStream(DecentralandTheGraphCompleteObjectStream):
    name = "accounts_ethereum"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_collections_url"]

    primary_keys = ["id"]
    object_returned = 'accounts'
    
    query = """
    query ($offset: Int!)
    {
        accounts(
            first: 1000,
            skip: $offset,
            orderBy:spent,
            orderDirection:desc
        ) {
            id
            address
            isCommitteeMember
            totalCurations
            sales
            purchases
            spent
            earned
            royalties
        }
    }
    """
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("address", th.StringType),
        th.Property("isCommitteeMember", th.BooleanType),
        th.Property("totalCurations", th.IntegerType),
        th.Property("sales", th.IntegerType),
        th.Property("purchases", th.IntegerType),
        th.Property("spent", th.StringType),
        th.Property("earned", th.StringType),
        th.Property("royalties", th.StringType),
    ).to_dict()
    

class PolygonAccountsStream(DecentralandTheGraphCompleteObjectStream):
    name = "accounts_polygon"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["polygon_api_url"]

    primary_keys = ["id"]
    object_returned = 'accounts'
    
    query = """
        query ($offset: Int!)
        {
            accounts(
                first: 1000,
                skip: $offset,
                orderBy:spent,
                orderDirection:desc
            ) {
                id
                address
                isCommitteeMember
                totalCurations
                sales
                purchases
                spent
                earned
                royalties
            }
        }
        """    

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("address", th.StringType),
        th.Property("isCommitteeMember", th.BooleanType),
        th.Property("totalCurations", th.IntegerType),
        th.Property("sales", th.IntegerType),
        th.Property("purchases", th.IntegerType),
        th.Property("spent", th.StringType),
        th.Property("earned", th.StringType),
        th.Property("royalties", th.StringType),
    ).to_dict()