"""GraphQL client handling, including DecentralandTheGraphStream base class."""
from typing import Optional
from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import GraphQLStream
from singer_sdk.exceptions import RetriableAPIError


class DecentralandTheGraphStream(GraphQLStream):
    """DecentralandTheGraph stream class."""
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["subgraph_url"]

    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[int]:
        """Return `start_date` config, or state if using timestamp replication."""
        if self.is_timestamp_replication_key:
            replication_key_value = self.get_starting_replication_key_value(context)
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
