"""Stream type classes for tap-decentraland-thegraph."""
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_thegraph.client import DecentralandTheGraphStream


class DaoEarningsStream(DecentralandTheGraphStream):
    name = "dao_earnings"

    primary_keys = ["id"]
    replication_key = 'date'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'analyticsDayDatas'

    query = """
    query ($updatedAt: Int!)
        {
            analyticsDayDatas(
                orderBy: id,
                orderDirection: asc,
                where:{
                    daoEarnings_gt: "0",
                    date_gt: $updatedAt,
                }
            )
            {
                id
                daoEarnings
                date
                volume
                sales
            }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("daoEarnings", th.StringType),
        th.Property("date", th.IntegerType),
        th.Property("volume", th.StringType),
        th.Property("sales", th.IntegerType),
    ).to_dict()
