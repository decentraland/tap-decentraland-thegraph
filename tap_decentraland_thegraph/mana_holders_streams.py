"""Stream type classes for tap-decentraland-thegraph."""

import requests, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import GraphQLStream

RESULTS_PER_PAGE = 1000

class ManaHolderStream(GraphQLStream):
    """ManaHolderStream stream class."""
    total_results_count = 0
    results_count = 0
    
    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or 0
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "offset": int(next_page_token),
        }


    def get_next_page_token(self, response, previous_token):
        if self.results_count == 0 or self.results_count < RESULTS_PER_PAGE:
            return None

        if self.total_results_count >= self.config["incremental_limit"]:
            self.logger.warn('Limit for this run reached')
            return None

        if previous_token is None:
            return RESULTS_PER_PAGE
        else:
            return previous_token + RESULTS_PER_PAGE
        

    
    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"][self.object_returned]
            self.results_count = len(results)
            self.total_results_count += self.results_count
            for row in results:
                yield row
        except Exception as err:
            self.logger.warn(f"(stream: {self.name}) Problem with response: {resp_json}")
            raise err
    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code >= 400,
        factor=2,
    )
    def _request_with_backoff(
        self, prepared_request, context: Optional[dict]
    ) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        if response.status_code in [401, 403]:
            self.logger.info("Failed request for {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        self.logger.debug("Response received successfully.")
        return response


class ETHManaStream(ManaHolderStream):
    name = "mana_holders_eth"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["eth_mana_holder_url"]

    primary_keys = ["id"]
    object_returned = 'accounts'
    
    query = """
    query ($offset: Int!)
    {
        accounts(
            first: 1000,
            offset: $offset,
            orderBy:mana,
            orderDirection:desc
        ) {
            id
            mana
        }
    }
    """

    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("mana", th.StringType, required=True),
    ).to_dict()


class PolygonManaStream(ManaHolderStream):
    name = "mana_holders_polygon"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["polygon_mana_holder_url"]

    primary_keys = ["id"]
    object_returned = 'accounts'
    
    query = """
    query ($offset: Int!)
    {
        accounts(
            first: 1000,
            offset: $offset,
            orderBy:mana,
            orderDirection:desc
        ) {
            id
            mana
        }
    }
    """

    
    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("mana", th.StringType, required=True),
    ).to_dict()
