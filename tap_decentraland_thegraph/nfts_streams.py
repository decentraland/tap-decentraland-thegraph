"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphStream


class WearablesStream(DecentralandTheGraphStream):
    name = "nfts_wearables"

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
                category: wearable,
                updatedAt_gte: $updatedAt
        })
        {
            id
            tokenId
            owner{
                id
            }
            tokenURI
            name
            image
            createdAt
            updatedAt
            wearable{
                representationId
                collection
                name
                description
                category
                rarity
                bodyShapes
            }
        }
    }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        bodyShapes = row['wearable']['bodyShapes']
        row['wearable']['bodyShapeMale'] = 'BaseMale' in bodyShapes
        row['wearable']['bodyShapeFemale'] = 'BaseFemale' in bodyShapes
        del row['wearable']['bodyShapes']

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
        th.Property("name", th.StringType),
        th.Property("image", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("wearable", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("representationId", th.StringType),
            th.Property("category", th.StringType),
            th.Property("collection", th.StringType),
            th.Property("rarity", th.StringType),
            th.Property("description", th.StringType),
            th.Property("bodyShapeMale", th.BooleanType),
            th.Property("bodyShapeFemale", th.BooleanType),
        ))
    ).to_dict()


class NamesStream(DecentralandTheGraphStream):
    name = "nfts_names"

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
                category: ens,
                updatedAt_gte: $updatedAt
        })
        {
            id
            tokenId
            owner{
                id
            }
            tokenURI
            name
            image
            createdAt
            updatedAt
            ens {
                caller
                beneficiary
                labelHash
                subdomain
            }
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
        th.Property("tokenId", th.StringType),
        th.Property("owner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenURI", th.StringType),
        th.Property("name", th.StringType),
        th.Property("image", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("ens", th.ObjectType(
            th.Property("caller", th.StringType),
            th.Property("beneficiary", th.StringType),
            th.Property("labelHash", th.StringType),
            th.Property("subdomain", th.StringType)
        ))
    ).to_dict()


class ParcelsStream(DecentralandTheGraphStream):
    name = "nfts_parcels"

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
                category: parcel,
                updatedAt_gte: $updatedAt
        })
        {
            id
            tokenId
            owner{
                id
            }
            tokenURI
            name
            image
            createdAt
            updatedAt
            parcel {
                x
                y
                estate{
                    id
                }
            }
        }
    }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['rowId'] = "|".join([row['id'], row['updatedAt']])

        """Convert to int"""
        row['parcel']['x'] = int(row['parcel']['x'])
        row['parcel']['y'] = int(row['parcel']['y'])
        if 'name' in row and isinstance(row['name'], str):
            row['name'] = row['name'].replace('\\', '\\\\')  # Escape backslashes
        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("rowId", th.StringType, required=True),
        th.Property("tokenId", th.StringType),
        th.Property("owner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenURI", th.StringType),
        th.Property("name", th.StringType),
        th.Property("image", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("parcel", th.ObjectType(
            th.Property("x", th.IntegerType),
            th.Property("y", th.IntegerType),
            th.Property("estate", th.ObjectType(
                th.Property("id", th.StringType)
            ))
        ))
    ).to_dict()


class EstatesStream(DecentralandTheGraphStream):
    name = "nfts_estates"

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
                category: estate,
                updatedAt_gte: $updatedAt
        })
        {
            id
            tokenId
            owner{
                id
            }
            tokenURI
            name
            image
            createdAt
            updatedAt
            estate {
                size
                parcels{
                    x
                    y
                }
            }
        }
    }

    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['rowId'] = "|".join([row['id'], row['updatedAt']])

        """Convert to int"""
        parcels = row['estate']['parcels']
        if parcels:
            converted = "|".join([f'{p["x"]},{p["y"]}' for p in parcels])
        else:
            converted = ''
        row['estate']['parcels'] = converted
        if 'name' in row and isinstance(row['name'], str):
            row['name'] = row['name'].replace('\\', '\\\\')  # Escape backslashes
        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("rowId", th.StringType, required=True),
        th.Property("tokenId", th.StringType),
        th.Property("owner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenURI", th.StringType),
        th.Property("name", th.StringType),
        th.Property("image", th.StringType),
        th.Property("createdAt", th.StringType),
        th.Property("updatedAt", th.StringType),
        th.Property("estate", th.ObjectType(
            th.Property("size", th.IntegerType),
            th.Property("parcels", th.StringType)
        ))
    ).to_dict()


class ItemsStream(DecentralandTheGraphStream):
    name = "items_ethereum"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_collections_url"]

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


class CollectionsEthereumStream(DecentralandTheGraphStream):
    name = "collections_ethereum"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_collections_url"]

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


class ItemsUniqueStream(DecentralandTheGraphStream):
    name = "items_ethereum_unique"
    primary_keys = ["id"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'items'

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_collections_url"]

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
        th.Property("creationFee", th.StringType),
        th.Property("uniqueCollectorsTotal", th.IntegerType),
        th.Property("firstListedAt", th.StringType),
        th.Property("volume", th.StringType),
        th.Property("metadata", th.ObjectType(
            th.Property("wearable", th.ObjectType(
                th.Property("bodyShapes", th.ArrayType(th.StringType)),
                th.Property("category", th.StringType),
                th.Property("description", th.StringType),
                th.Property("name", th.StringType),

            )),
            th.Property("emote", th.ObjectType(
                th.Property("bodyShapes", th.ArrayType(th.StringType)),
                th.Property("category", th.StringType),
                th.Property("description", th.StringType),
                th.Property("hasGeometry", th.BooleanType),
                th.Property("hasSound", th.BooleanType),
                th.Property("loop", th.BooleanType),
                th.Property("name", th.StringType),

            ))
        ))
    ).to_dict()
