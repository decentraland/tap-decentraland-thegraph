"""Stream type classes for tap-decentraland-thegraph."""

import requests, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphCompleteObjectStream


class ETHManaStream(DecentralandTheGraphCompleteObjectStream):
    name = "mana_holders_eth"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_mana_holder_url"]

    primary_keys = ["id"]
    object_returned = 'accounts'
    
    query = """
    query ($offset: Int!)
    {
        accounts(
            first: 1000,
            offset: $offset,
            orderBy:mana,
            orderDirection:desc
        ) {
            id
            mana
        }
    }
    """

    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("mana", th.StringType, required=True),
    ).to_dict()


class PolygonManaStream(DecentralandTheGraphCompleteObjectStream):
    name = "mana_holders_polygon"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["polygon_mana_holder_url"]

    primary_keys = ["id"]
    object_returned = 'accounts'
    
    query = """
    query ($offset: Int!)
    {
        accounts(
            first: 1000,
            offset: $offset,
            orderBy:mana,
            orderDirection:desc
        ) {
            id
            mana
        }
    }
    """

    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("mana", th.StringType, required=True),
    ).to_dict()
