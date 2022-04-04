"""Stream type classes for tap-decentraland-thegraph."""

import requests
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
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


class PoapsMetadata(BaseAPIStream):
    name = "poaps_metadata"
    path = "/paginated-events"

    RESULTS_PER_PAGE = 500

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["poaps_details_url"]

    primary_keys = ['id']
    replication_method = "FULL_TABLE"
    is_sorted = True
    records_jsonpath: str = "$.items[*]"
    next_page_token_jsonpath: str = "$.items[-1:].start_date"
    
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        all_matches = extract_jsonpath(
            self.next_page_token_jsonpath, response.json()
        )
        first_match = next(iter(all_matches), None)
        next_page_token = first_match

        if next_page_token == previous_token:
            return None

        return next_page_token

    def get_url_params(
        self,
        context: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        next_timestamp = datetime(2000,1,1)
        if next_page_token:
            next_timestamp = datetime.strptime(next_page_token, '%d-%b-%Y')
        self.logger.info(f"Time: {next_timestamp}")
        return {"limit": self.RESULTS_PER_PAGE, "from_date": next_timestamp.strftime("%Y-%m-%dT%H:%M:%Sz"), "sort_field": "start_date", "sort_dir":"asc"}

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Add hash"""
        row['start_date'] = int(datetime.strptime(row['start_date'], '%d-%b-%Y').timestamp())
        return row

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
        th.Property("start_date", th.IntegerType),
        th.Property("end_date", th.StringType),
        th.Property("expiry_date", th.StringType),
        th.Property("from_admin", th.BooleanType),
        th.Property("virtual_event", th.BooleanType),
        th.Property("event_template_id", th.IntegerType),
        th.Property("event_host_id", th.IntegerType),
        th.Property("private_event", th.BooleanType),
    ).to_dict()