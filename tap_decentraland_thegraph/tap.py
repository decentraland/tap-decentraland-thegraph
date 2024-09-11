"""DecentralandTheGraph tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_decentraland_thegraph.orders_streams import (
    WearablesOrdersStream,
    ParcelsOrdersStream,
    EstatesOrdersStream,
    EstatesHistoricalStream,
    NamesOrdersStream,
)

from tap_decentraland_thegraph.bids_streams import (
    WearablesBidsStream,
    ParcelsBidsStream,
    EstatesBidsStream,
    EstatesBidsHistoricalStream,
    NamesBidsStream,
)

from tap_decentraland_thegraph.nfts_streams import (
    WearablesStream,
    EstatesStream,
    ParcelsStream,
    NamesStream,
    ItemsStream,
    ItemsUniqueStream,
    CollectionsEthereumStream
)

from tap_decentraland_thegraph.nfts_streams_polygon import (
    WearablesPolygonStream,
    CollectionsPolygonStream,
    ItemsPolygonStream,
    ItemsPolygonUniqueStream
)

from tap_decentraland_thegraph.polygon_collections import (
    MintsPolygonStream,
    MintsPolygonStreamV2,
    CurationsPolygonStream
)

from tap_decentraland_thegraph.orders_streams_polygon import (
    WearablesOrdersPolygonStream,
    WearablesPrimarySalesPolygonStream,
)

from tap_decentraland_thegraph.bids_streams_polygon import (
    WearablesBidsPolygonStream,
)

from tap_decentraland_thegraph.account_streams import (
    LogStreams,
)

from tap_decentraland_thegraph.poaps import (
    PoapsXdai,
    PoapsMetadata
)

from tap_decentraland_thegraph.accounts_streams import (
    ETHAccountsStream,
    PolygonAccountsStream
)
from tap_decentraland_thegraph.sales_streams import (
    ETHSalesStream,
    PolygonSalesStream
)
from tap_decentraland_thegraph.rentals_streams import (
    RentalsStream
)

STREAM_TYPES = [
    WearablesBidsStream,
    WearablesOrdersStream,
    ParcelsOrdersStream,
    EstatesOrdersStream,
    EstatesHistoricalStream,
    NamesOrdersStream,
    WearablesStream,
    WearablesPolygonStream,
    EstatesStream,
    ParcelsStream,
    NamesStream,
    ParcelsBidsStream,
    EstatesBidsStream,
    EstatesBidsHistoricalStream,
    NamesBidsStream,
    WearablesOrdersPolygonStream,
    WearablesBidsPolygonStream,
    LogStreams,
    CollectionsPolygonStream,
    ItemsPolygonStream,
    ItemsPolygonUniqueStream,
    WearablesPrimarySalesPolygonStream,
    PoapsXdai,
    PoapsMetadata,
    ItemsStream,
    ItemsUniqueStream,
    ETHAccountsStream,
    PolygonAccountsStream,
    ETHSalesStream,
    PolygonSalesStream,
    MintsPolygonStream,
    CollectionsEthereumStream,
    RentalsStream,
    MintsPolygonStreamV2,
    CurationsPolygonStream
]


class TapDecentralandTheGraph(Tap):
    """DecentralandTheGraph tap class."""
    name = "tap-decentraland-thegraph"

    config_jsonschema = th.PropertiesList(
        th.Property("start_updated_at", th.IntegerType, default=1),
        th.Property("subgraph_url", th.StringType,
                    default="https://subgraph.decentraland.org"),
        th.Property("account_subgraph_paths", th.ArrayType(th.StringType),
                    default=["mana-ethereum-mainnet", "mana-matic-mainnet"]),
        th.Property("max_rows_per_run", th.IntegerType, default=1000),
        th.Property("api_url", th.StringType,
                    default='https://subgraph.decentraland.org/marketplace'),
        th.Property("polygon_collections_url", th.StringType,
                    default='https://subgraph.decentraland.org/collections-matic-mainnet'),
        th.Property("incremental_limit", th.IntegerType, default=50000),
        th.Property("eth_mana_holder_url", th.StringType,
                    default='https://subgraph.decentraland.org/mana-ethereum-mainnet'),
        th.Property("polygon_mana_holder_url", th.StringType,
                    default='https://subgraph.decentraland.org/mana-matic-mainnet'),
        th.Property("poaps_xdai_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/poap-xyz/poap-xdai'),
        th.Property("poaps_details_url", th.StringType, default='http://api.poap.xyz'),
        th.Property("eth_collections_url", th.StringType,
                    default='https://subgraph.decentraland.org/collections-ethereum-mainnet'),
        th.Property("rentals_url", th.StringType,
                    default='https://subgraph.decentraland.org/rentals-ethereum-mainnet'),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
