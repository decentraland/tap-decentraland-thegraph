"""Stream type classes for tap-decentraland-thegraph."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphPolygonStream, BaseAPIStream


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


    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "poap_id": record['id']
        }


class PoapsMetadata(BaseAPIStream):
    name = "poaps_metadata"
    path = "/events"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["poaps_details_url"]

    primary_keys = ['id']

    def parse_response(self, response) -> Iterable[dict]:
        """Parse Tiles rows"""

        data =response.json()
        for t in data:
            yield t

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("fancy_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("event_url", th.StringType),
        th.Property("image_url", th.StringType),
        th.Property("country", th.StringType),
        th.Property("city", th.StringType),
        th.Property("description", th.StringType),
        th.Property("year", th.IntegerType),
        th.Property("start_date", th.StringType),
        th.Property("end_date", th.StringType),
        th.Property("expiry_date", th.StringType),
        th.Property("created_date", th.StringType),
        th.Property("from_admin", th.BooleanType),
        th.Property("virtual_event", th.BooleanType),
        th.Property("event_template_id", th.IntegerType),
        th.Property("event_host_id", th.IntegerType),
        th.Property("private_event", th.BooleanType),
    ).to_dict()