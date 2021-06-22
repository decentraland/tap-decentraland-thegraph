"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphPolygonStream


class WearablesBidsPolygonStream(DecentralandTheGraphPolygonStream):
    name = "bids_polygon_wearables"

    primary_keys = ["id"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'bids'
    
    query = """
    query ($updatedAt: Int!)
        {
            bids(
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
                seller
                price
                bidder
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
        th.Property("seller", th.StringType),
        th.Property("price", th.StringType),
        th.Property("bidder", th.StringType),
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



