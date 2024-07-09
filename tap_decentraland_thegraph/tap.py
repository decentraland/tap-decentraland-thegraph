"""DecentralandTheGraph tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_decentraland_thegraph.collections import (
    MintsStream,
    OrdersStream,
    BidsStream,
    CollectionsStream,
    ItemsStream
)

from tap_decentraland_thegraph.marketplace import (
    MarketplaceOrdersStream
)


STREAM_TYPES = [
    MintsStream,
    OrdersStream,
    BidsStream,
    CollectionsStream,
    ItemsStream,
    MarketplaceOrdersStream
]


class TapDecentralandTheGraph(Tap):
    """DecentralandTheGraph tap class."""
    name = "tap-decentraland-thegraph"

    config_jsonschema = th.PropertiesList(
        th.Property("start_updated_at", th.IntegerType, default=1),
        th.Property("subgraph_url", th.StringType,
                    default="https://subgraph.decentraland.org", required=True),
        th.Property("collection_paths", th.ArrayType(th.StringType), default=[
                    "collections-ethereum-mainnet", "collections-matic-mainnet"], required=True)
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
