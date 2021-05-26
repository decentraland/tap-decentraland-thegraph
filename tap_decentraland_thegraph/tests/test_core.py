"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_decentraland_thegraph.tap import TapDecentralandTheGraph

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapDecentralandTheGraph,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
