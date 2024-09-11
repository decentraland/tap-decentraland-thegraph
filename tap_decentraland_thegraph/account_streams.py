"""Stream type classes for tap-decentraland-thegraph."""

from datetime import datetime
from typing import Iterable, Optional
import requests
from singer_sdk.streams import GraphQLStream
from singer_sdk import typing as th


class AccountStream(GraphQLStream):
    @property
    def partitions(self):
        return [{"path": path} for path in self.config["account_subgraph_paths"]]

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["subgraph_url"]

    def get_url(self, context: Optional[dict]) -> str:
        path = context["path"]
        return super().get_url(context) + f"/{path}"


class LogStreams(AccountStream):
    name = "account_logs"
    primary_keys = ["id"]
    replication_key = "time"
    replication_method = "INCREMENTAL"
    is_sorted = True
    records_jsonpath = '$.data.logs[*]'
    next_page_token_jsonpath = '$.data.logs[-1].time'
    query = """
        query ($timestampFrom: BigInt, $timestampTo: BigInt) {
            logs(
                first: 100
                orderBy: time
                orderDirection: asc
                where: { 
                    time_gt: $timestampFrom
                    time_lt: $timestampTo 
                    }
            ) {
                id
                from
                to
                index
                value
                time
                involved
                txHash
            }
        }
    """
    records_fetched = 0
    last_timestamp = None
    MAX_ROWS_PER_RUN = 1000

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        rows = response.json().get("data").get("logs")
        self.records_fetched += len(rows)
        self.logger.info(f"Total records fetched: {self.records_fetched}")

        for row in rows:
            self.last_timestamp = int(row.get("time"))
            yield row

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[int]:
        # If maximum rows fetched, return the same timestamp to force an empty array in the next response.
        row_limit = self.config["max_rows_per_run"] if "max_rows_per_run" in self.config else self.MAX_ROWS_PER_RUN

        if (self.records_fetched >= row_limit):
            return self.last_timestamp

        return int(datetime.now().timestamp())

    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token
        replication_key_value = self.get_starting_replication_key_value(
            context=partition)
        signpost = self.get_replication_key_signpost(context=partition)

        if next_page_token:
            next_timestamp = next_page_token
        elif replication_key_value:
            next_timestamp = replication_key_value
        else:
            next_timestamp = 1

        return {
            "timestampFrom": int(next_timestamp),
            "timestampTo": int(signpost)
        }

    def post_process(self, row: dict, context: Optional[dict]) -> Optional[dict]:
        path = context["path"]

        if path == "mana-ethereum-mainnet":
            row['network'] = 'Ethereum'

        if path == "mana-matic-mainnet":
            row['network'] = 'Polygon'

        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("from", th.StringType),
        th.Property("to", th.StringType),
        th.Property("index", th.StringType),
        th.Property("value", th.StringType),
        th.Property("time", th.StringType),
        th.Property("involved", th.StringType),
        th.Property("txHash", th.StringType),
        th.Property("network", th.StringType)
    ).to_dict()
