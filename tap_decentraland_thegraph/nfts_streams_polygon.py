"""Stream type classes for tap-decentraland-thegraph."""

import requests
import backoff
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
                itemType
                wearable{
                    id
                    collection
                    name
                    description
                    category
                    rarity
                    bodyShapes
                }   
                emote {
                    id
                    name
                    description
                    collection
                    category
                    rarity
                    bodyShapes
                    hasGeometry
                    hasSound
                    loop
                }
            }
        }
    }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        if 'wearable' in row['metadata'] and row['metadata']['wearable'] is not None:
            if 'bodyShapes' in row['metadata']['wearable'] and row['metadata']['wearable']['bodyShapes'] is not None:
                bodyShapes = row['metadata']['wearable']['bodyShapes']
                row['metadata']['wearable']['bodyShapeMale'] = 'BaseMale' in bodyShapes
                row['metadata']['wearable']['bodyShapeFemale'] = 'BaseFemale' in bodyShapes
                del row['metadata']['wearable']['bodyShapes']
        else:
            row['metadata']['wearable'] = {}

        if 'emote' in row['metadata'] and row['metadata']['emote'] is not None:
            if 'bodyShapes' in row['metadata']['emote'] and row['metadata']['emote']['bodyShapes'] is not None:
                bodyShapes = row['metadata']['emote']['bodyShapes']
                row['metadata']['emote']['bodyShapeMale'] = 'BaseMale' in bodyShapes
                row['metadata']['emote']['bodyShapeFemale'] = 'BaseFemale' in bodyShapes
                del row['metadata']['emote']['bodyShapes']
        else:
            row['metadata']['emote'] = {}

        """Generate row id"""
        row['rowId'] = "|".join([row['id'], row['updatedAt']])
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
            th.Property("itemType", th.StringType),
            th.Property("wearable", th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("category", th.StringType),
                th.Property("collection", th.StringType),
                th.Property("rarity", th.StringType),
                th.Property("description", th.StringType),
                th.Property("bodyShapeMale", th.BooleanType),
                th.Property("bodyShapeFemale", th.BooleanType),
            )),
            th.Property("emote", th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("category", th.StringType),
                th.Property("collection", th.StringType),
                th.Property("rarity", th.StringType),
                th.Property("description", th.StringType),
                th.Property("bodyShapeMale", th.BooleanType),
                th.Property("bodyShapeFemale", th.BooleanType),
                th.Property("hasGeometry", th.BooleanType),
                th.Property("hasSound", th.BooleanType),
                th.Property("loop", th.BooleanType),
            ))
        ))
    ).to_dict()


class CollectionsPolygonStream(DecentralandTheGraphPolygonStream):
    name = "collections_polygon"

    primary_keys = ["rowId"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'collections'

    query = """
    query ($updatedAt: Int!)
        {
        collections (
            first: 1000,
            orderBy: updatedAt,
            orderDirection: asc,
            where:{
                updatedAt_gte: $updatedAt
        })
        {
            id
            owner
            creator
            name
            symbol
            isCompleted
            isApproved
            isEditable
            minters
            managers
            urn
            itemsCount
            createdAt
            updatedAt
            reviewedAt
            searchIsStoreMinter
            searchText
        }
    }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['rowId'] = "|".join([row['id'], row['updatedAt']])

        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("rowId", th.StringType, required=True),
        th.Property("owner", th.StringType),
        th.Property("creator", th.StringType),
        th.Property("name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("isCompleted", th.BooleanType),
        th.Property("isApproved", th.BooleanType),
        th.Property("isEditable", th.BooleanType),
        th.Property("minters", th.ArrayType(th.StringType)),
        th.Property("managers", th.ArrayType(th.StringType)),
        th.Property("urn", th.StringType),
        th.Property("itemsCount", th.IntegerType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("reviewedAt", th.StringType),
        th.Property("searchIsStoreMinter", th.BooleanType),
        th.Property("searchText", th.StringType)
    ).to_dict()


class ItemsPolygonStream(DecentralandTheGraphPolygonStream):
    name = "items_polygon"

    primary_keys = ["rowId"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'items'

    query = """
    query ($updatedAt: Int!)
        {
        items (
            first: 1000,
            orderBy: updatedAt,
            orderDirection: asc,
            where:{
                updatedAt_gte: $updatedAt
        })
        {
            id
            collection{
                id
            }
            blockchainId
            creator
            itemType
            totalSupply
            maxSupply
            rarity
            available
            price
            beneficiary
            contentHash
            URI
            image
            minters
            managers
            urn
            createdAt
            updatedAt
            creationFee
        }
    }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['rowId'] = "|".join([row['id'], row['updatedAt']])

        # Convert ints
        row['totalSupply'] = int(row['totalSupply'])
        row['maxSupply'] = int(row['maxSupply'])
        row['available'] = int(row['available'])

        # If Price is a long number null the value
        # so it doesn't crash when inserting
        if len(row['price']) > 32:
            row['price'] = None
        else:
            row['price'] = int(row['price'])

        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("rowId", th.StringType, required=True),
        th.Property("collection", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("blockchainId", th.StringType),
        th.Property("creator", th.StringType),
        th.Property("itemType", th.StringType),
        th.Property("totalSupply", th.IntegerType),
        th.Property("maxSupply", th.IntegerType),
        th.Property("rarity", th.StringType),
        th.Property("available", th.IntegerType),
        th.Property("price", th.IntegerType),
        th.Property("beneficiary", th.StringType),
        th.Property("contentHash", th.StringType),
        th.Property("URI", th.StringType),
        th.Property("image", th.StringType),
        th.Property("minters", th.ArrayType(th.StringType)),
        th.Property("managers", th.ArrayType(th.StringType)),
        th.Property("urn", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("creationFee", th.StringType)
    ).to_dict()


class ItemsPolygonUniqueStream(DecentralandTheGraphPolygonStream):
    name = "items_polygon_unique"
    primary_keys = ["id"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'items'

    query = """
        query ($updatedAt: Int!) 
        {
        items(
            first: 1000
            orderBy: updatedAt
            orderDirection: asc
            where: {
                updatedAt_gte: $updatedAt
            })
         {
            id
            collection {
                id
            }
            blockchainId
            creator
            itemType
            totalSupply
            maxSupply
            rarity
            available
            price
            beneficiary
            contentHash
            URI
            image
            minters
            managers
            urn
            createdAt
            updatedAt
            creationFee
            uniqueCollectorsTotal
            firstListedAt
            volume
            metadata {
                wearable {
                    bodyShapes
                    category
                    description
                    name
                }
                emote {
                    bodyShapes
                    category
                    description
                    hasGeometry
                    hasSound
                    loop
                    name
                    
                }
            }
        }
        }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        # Convert ints
        row['totalSupply'] = int(row['totalSupply'])
        row['maxSupply'] = int(row['maxSupply'])
        row['available'] = int(row['available'])

        if len(row['price']) > 32:
            row['price'] = None
        else:
            row['price'] = int(row['price'])

        if 'wearable' in row['metadata'] and row['metadata']['wearable'] is not None:
            bodyShapes = row['metadata']['wearable']['bodyShapes']

            row['category'] = row['metadata']['wearable']['category']
            row['description'] = row['metadata']['wearable']['description']
            row['name'] = row['metadata']['wearable']['name']
            row['is_male_shape'] = 'BaseMale' in bodyShapes
            row['is_female_shape'] = 'BaseFemale' in bodyShapes
            row['hasGeometry'] = False
            row['hasSound'] = False
            row['loop'] = False
            del row['metadata']['wearable']

        if 'emote' in row['metadata'] and row['metadata']['emote'] is not None:
            bodyShapes = row['metadata']['emote']['bodyShapes']

            row['category'] = row['metadata']['emote']['category']
            row['description'] = row['metadata']['emote']['description']
            row['name'] = row['metadata']['emote']['name']
            row['is_male_shape'] = 'BaseMale' in bodyShapes
            row['is_female_shape'] = 'BaseFemale' in bodyShapes
            row['hasGeometry'] = row['metadata']['emote']['hasGeometry']
            row['hasSound'] = row['metadata']['emote']['hasSound']
            row['loop'] = row['metadata']['emote']['loop']
            del row['metadata']['emote']

        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("collection", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("blockchainId", th.StringType),
        th.Property("creator", th.StringType),
        th.Property("itemType", th.StringType),
        th.Property("totalSupply", th.IntegerType),
        th.Property("maxSupply", th.IntegerType),
        th.Property("rarity", th.StringType),
        th.Property("available", th.IntegerType),
        th.Property("price", th.IntegerType),
        th.Property("beneficiary", th.StringType),
        th.Property("contentHash", th.StringType),
        th.Property("URI", th.StringType),
        th.Property("image", th.StringType),
        th.Property("urn", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("creationFee", th.StringType),
        th.Property("uniqueCollectorsTotal", th.IntegerType),
        th.Property("firstListedAt", th.StringType),
        th.Property("volume", th.StringType),
        th.Property("is_male_shape", th.BooleanType),
        th.Property("is_female_shape", th.BooleanType),
        th.Property("category", th.StringType),
        th.Property("description", th.StringType),
        th.Property("name", th.StringType),
        th.Property("hasGeometry", th.BooleanType),
        th.Property("hasSound", th.BooleanType),
        th.Property("loop", th.BooleanType)
    ).to_dict()
