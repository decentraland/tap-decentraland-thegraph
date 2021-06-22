"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphStream



class WearablesBidsStream(DecentralandTheGraphStream):
    name = "bids_wearables"

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
                    category:wearable,
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
                    wearable {
                        name
                        representationId
                        collection
                        rarity
                        description
                        bodyShapes
                    }
                }
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        bodyShapes = row['nft']['wearable']['bodyShapes']
        row['nft']['wearable']['bodyShapeMale'] = 'BaseMale' in bodyShapes
        row['nft']['wearable']['bodyShapeFemale'] = 'BaseFemale' in bodyShapes
        del row['nft']['wearable']['bodyShapes']
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
            th.Property("wearable", th.ObjectType(
                th.Property("name", th.StringType),
                th.Property("representationId", th.StringType),
                th.Property("collection", th.StringType),
                th.Property("rarity", th.StringType),
                th.Property("description", th.StringType),
                th.Property("bodyShapeMale", th.BooleanType),
                th.Property("bodyShapeFemale", th.BooleanType),
            ))
        )),
    ).to_dict()




class ParcelsBidsStream(DecentralandTheGraphStream):
    name = "bids_parcels"

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
                    category:parcel,
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
                    parcel {
                        x
                        y
                    }
                }
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert x/y to integers"""
        row['nft']['parcel']['x'] = int(row['nft']['parcel']['x'])
        row['nft']['parcel']['y'] = int(row['nft']['parcel']['y'])
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
            th.Property("parcel", th.ObjectType(
                th.Property("x", th.IntegerType),
                th.Property("y", th.IntegerType),
            ))
        )),
    ).to_dict()

class EstatesBidsStream(DecentralandTheGraphStream):
    name = "bids_estates"

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
                    category:estate,
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
                }
            }
        }
    """

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "estateId": record["nft"]['id'],
            "blockNumber": record["blockNumber"]
        }

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
        )),
    ).to_dict()

class EstatesBidsHistoricalStream(DecentralandTheGraphStream):
    """Child stream that gets historical snapshot"""
    name = "historical_snapshot_estates_bids"

    # Child stream
    parent_stream_type = EstatesBidsStream

    primary_keys = ["rowId"]
    replication_key = 'rowId'
    replication_method = "INCREMENTAL"
    ignore_parent_replication_keys = True
    is_sorted = True
    object_returned = 'estates'
    dedupe = False
    onlyonerow = True
    
    query = """
    query ($estateId: ID!, $blockNumber: Int!)
    {
        estates(
            first: 1,
            where:{id: $estateId},
            block:{number: $blockNumber}
        ) {
            id
            tokenId
            parcels {
                x
                y
            }
            size
        }
    }

    """


    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        
        return {
            "estateId": partition['estateId'],
            "blockNumber": int(partition['blockNumber'])
        }

    
    def get_next_page_token(self, response, previous_token):
        if self.results_count == 0:
            return None
        if previous_token:
            return None

        return self.latest_timestamp

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert parcels into psv and adds block number"""
        parcels = row['parcels']
        if parcels:
            converted = "|".join([f'{p["x"]},{p["y"]}' for p in parcels])
        else:
            converted = ''
        row['parcels'] = converted
        row['blockNumber'] = context['blockNumber']
        row['rowId'] = "|".join([row['id'],row['blockNumber']])
        return row
    
    schema = th.PropertiesList(
        th.Property("rowId", th.StringType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("blockNumber", th.StringType),
        th.Property("tokenId", th.StringType),
        th.Property("size", th.IntegerType),
        th.Property("parcels", th.StringType),
    ).to_dict()


class NamesBidsStream(DecentralandTheGraphStream):
    name = "bids_names"

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
                    category:ens,
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
                    ens {
                        id
                        tokenId
                        caller
                        beneficiary
                        labelHash
                        subdomain
                        createdAt
                    }
                }
            }
        }
    """
    
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
            th.Property("ens", th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("tokenId", th.StringType),
                th.Property("caller", th.StringType),
                th.Property("beneficiary", th.StringType),
                th.Property("labelHash", th.StringType),
                th.Property("subdomain", th.StringType),
                th.Property("createdAt", th.StringType)
            ))
        )),
    ).to_dict()
