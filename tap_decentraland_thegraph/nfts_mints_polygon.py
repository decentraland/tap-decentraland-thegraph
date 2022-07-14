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
        row['rowId'] = "|".join([row['id'],row['timestamp']])
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
