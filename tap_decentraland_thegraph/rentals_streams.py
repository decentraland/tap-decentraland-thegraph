"""Stream type classes for tap-decentraland-thegraph."""

import requests, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphStream


class RentalsStream(DecentralandTheGraphStream):
    name = "rentals"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["rentals_url"]

    primary_keys = ["id"]
    replication_key = 'updatedAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'rentals'
    
    query = """
    query ($updatedAt: Int!)
    {
        rentals(
            first: 1000,
            orderBy: updatedAt,
            orderDirection: asc
            where:{
                updatedAt_gte: $updatedAt                
                }
        ) {
                id
                contractAddress
                tokenId
                lessor
                tenant
                operator
                rentalDays
                startedAt
                endsAt
                updatedAt
                pricePerDay
                sender
                ownerHasClaimedAsset
                isExtension
                isActive
                rentalContractAddress
        }
    }
    """


    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert parcels into psv and adds block number"""
        if 'rentalDays' in row:
            row['rentalDays'] = int(row['rentalDays'])
        if 'startedAt' in row:
            row['startedAt'] = int(row['startedAt'])
        if 'endsAt' in row:
            row['endsAt'] = int(row['endsAt'])
        return row

    schema = th.PropertiesList(
        # id: Rental ID (Each succesful rent has a unique identifier)
        th.Property("id", th.StringType, required=True),
        # contractAddress: The address of the ERC721 contract (LAND, Estates, any other ERC721 that can be rented with this contract)
        th.Property("contractAddress", th.StringType),
        # rentalContractAddress: The address of the Rentals contract. will probably never change, but if for some reason we have to use a different, its useful to differentiate from which contract the rental was made
        th.Property("rentalContractAddress", th.StringType),
        # tokenId: Id of the NFT being rented
        th.Property("tokenId", th.StringType),
        # lessor: ETH address of the owner of the land/estate
        th.Property("lessor", th.StringType),
        # tenant: ETH address of the renter of the land/estate
        th.Property("tenant", th.StringType),
        # operator: The address of the user that was given update operator permissions for the LAND/Estate by the tenant.
        th.Property("operator", th.StringType),
        # rentalDays: How many days is this property rented for
        th.Property("rentalDays", th.IntegerType),
        # startedAt: When does the rent start (Unix timestamp)
        th.Property("startedAt", th.IntegerType),
        # endsAt: When does the rent end (Unix timestamp)
        th.Property("endsAt", th.IntegerType),
        # updatedAt: When was this rent last updated
        th.Property("updatedAt", th.StringType),
        # pricePerDay: How much mana has been paid per day
        th.Property("pricePerDay", th.StringType),
        # sender: The address that sent the transaction to start the rental
        th.Property("sender", th.StringType),
        # ownerHasClaimedAsset: If the owner claimed the asset back on that rental (after it ends)
        th.Property("ownerHasClaimedAsset", th.BooleanType),
        # isExtension: Is this rent an extension of a previous rent?
        th.Property("isExtension", th.BooleanType),
        # isActive: Rental entities are historical for the same nft, isActive indicates that it is the last executed rental for the nft (Could be a finished rental)
        th.Property("isActive", th.BooleanType),
    ).to_dict()