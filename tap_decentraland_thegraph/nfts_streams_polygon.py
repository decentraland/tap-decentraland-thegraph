"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphPolygonStream


class WearablesPolygonStream(DecentralandTheGraphPolygonStream):
    name = "nfts_wearables_polygon"

    primary_keys = ["rowId"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'nfts'
    
    query = """
    query ($updatedAt: Int!)
        {
        nfts (
            first: 1000,
            orderBy: updatedAt,
            orderDirection: asc,
            where:{
                updatedAt_gte: $updatedAt
        })
        {
            id
            tokenId
            owner{
                id
            }
            tokenURI
            image
            createdAt
            updatedAt
            metadata {
                wearable{
                    id
                    collection
                    name
                    description
                    category
                    rarity
                    bodyShapes
                }
            }
        }
    }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        bodyShapes = row['metadata']['wearable']['bodyShapes']
        row['metadata']['wearable']['bodyShapeMale'] = 'BaseMale' in bodyShapes
        row['metadata']['wearable']['bodyShapeFemale'] = 'BaseFemale' in bodyShapes
        del row['metadata']['wearable']['bodyShapes']

        """Generate row id"""
        row['rowId'] = "|".join([row['id'],row['updatedAt']])
        return row

    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("rowId", th.StringType, required=True),
        th.Property("tokenId", th.StringType),
        th.Property("owner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenURI", th.StringType),
        th.Property("image", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("metadata", th.ObjectType(
            th.Property("wearable", th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("category", th.StringType),
                th.Property("collection", th.StringType),
                th.Property("rarity", th.StringType),
                th.Property("description", th.StringType),
                th.Property("bodyShapeMale", th.BooleanType),
                th.Property("bodyShapeFemale", th.BooleanType),
            ))
        ))
    ).to_dict()
