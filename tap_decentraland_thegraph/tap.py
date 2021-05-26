"""DecentralandTheGraph tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_decentraland_thegraph.streams import (
    DecentralandTheGraphStream,
    WearablesStream,
    ParcelsStream,
    EstatesStream,
    EstatesHistoricalStream,
    NamesStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    WearablesStream,
    ParcelsStream,
    EstatesStream,
    EstatesHistoricalStream,
    NamesStream,
]


class TapDecentralandTheGraph(Tap):
    """DecentralandTheGraph tap class."""
    name = "tap-decentraland-thegraph"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("start_updated_at", th.IntegerType, default=1546300800), #2019-01-01
        th.Property("api_url", th.StringType, default='https://api.thegraph.com/subgraphs/name/decentraland/marketplace'),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
