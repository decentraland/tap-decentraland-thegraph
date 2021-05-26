"""GraphQL client handling, including DecentralandTheGraphStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import GraphQLStream


class DecentralandTheGraphStream(GraphQLStream):
    """DecentralandTheGraph stream class."""

    is_timestamp_replication_key = True
    latest_timestamp = None
    results_count = None
    results_keys = set()
    dedupe = True
    onlyonerow = False

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]
    
    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[int]:
        """Return `start_date` config, or state if using timestamp replication."""
        if self.is_timestamp_replication_key:
            replication_key_value = self._starting_replication_key_value(context)
            if replication_key_value:
                return replication_key_value

        if "start_updated_at" in self.config:
            return self.config["start_updated_at"]

        return None


    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or self.get_starting_timestamp(partition)
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "updatedAt": int(next_page_token),
        }


    def get_next_page_token(self, response, previous_token):
        if self.results_count == 0:
            return None
        if previous_token and self.latest_timestamp == previous_token:
            return None

        return self.latest_timestamp
        

    
    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"][self.object_returned]
            self.results_count = len(results)
            for row in results:

                if self.onlyonerow == False:
                    #Update timestamp
                    if self.latest_timestamp is None or row[self.replication_key] > self.latest_timestamp:
                        self.latest_timestamp = row[self.replication_key]
                
                yield row
        except Exception as err:
            self.logger.warn(f"(stream: {self.name}) Problem with response: {resp_json}")
            raise err

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        Modified to detect dupes
        """
        for row in self.request_records(context):
            row = self.post_process(row, context)
            row_key = "|".join([v for k,v in row.items() if k in self.primary_keys])
            if row_key in self.results_keys and self.dedupe:
                # Because thegraph doesn't allow for reliable pagination, sometimes you could get
                # duplicate rows from the same second.
                self.logger.warn(f"(stream: {self.name}) skipping duplicate {row_key}")
                continue

            #Add key as processed to avoid dupes
            if self.dedupe:
                self.results_keys.add(row_key)
            yield row

