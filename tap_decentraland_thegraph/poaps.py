"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphPolygonStream


class PoapsXdai(DecentralandTheGraphPolygonStream):
    name = "poaps_xdai"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["poaps_xdai_url"]


    primary_keys = ["id"]
    replication_key = 'created'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'events'
    
    query = """
    query ($updatedAt: Int!)
    {
        events (
            first: 1000,
            offset: $offset,
            orderBy: created,
            orderDirection: asc,
            where:{
                created_gte: $updatedAt
            }
        ){
            id
            tokenCount
            transferCount
            created
        }
    }
    """
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("tokenCount", th.StringType),
        th.Property("transferCount", th.StringType),
        th.Property("created", th.StringType),
    ).to_dict()