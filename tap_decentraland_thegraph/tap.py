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

from tap_decentraland_thegraph.nfts_mints_polygon import (
    MintsPolygonStream,
)

from tap_decentraland_thegraph.orders_streams_polygon import (
    WearablesOrdersPolygonStream,
    WearablesPrimarySalesPolygonStream,
)

from tap_decentraland_thegraph.bids_streams_polygon import (
    WearablesBidsPolygonStream,
)

from tap_decentraland_thegraph.mana_holders_streams import (
    ETHManaStream,
    PolygonManaStream,
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
    ETHManaStream,
    PolygonManaStream,
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
    RentalsStream
]


class TapDecentralandTheGraph(Tap):
    """DecentralandTheGraph tap class."""
    name = "tap-decentraland-thegraph"

    config_jsonschema = th.PropertiesList(
        th.Property("start_updated_at", th.IntegerType, default=1),
        th.Property("api_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/decentraland/marketplace'),
        th.Property("polygon_collections_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/decentraland/collections-matic-mainnet'),
        th.Property("incremental_limit", th.IntegerType, default=50000),
        th.Property("eth_mana_holder_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/decentraland/mana-ethereum-mainnet'),
        th.Property("polygon_mana_holder_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/decentraland/mana-matic-mainnet'),
        th.Property("poaps_xdai_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/poap-xyz/poap-xdai'),
        th.Property("poaps_details_url", th.StringType, default='http://api.poap.xyz'),
        th.Property("eth_collections_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/decentraland/collections-ethereum-mainnet'),
        th.Property("rentals_url", th.StringType,
                    default='https://api.thegraph.com/subgraphs/name/decentraland/rentals-ethereum-mainnet'),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
