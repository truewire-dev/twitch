"""Shared fixtures: a real `Twitch` client pointed at the local mock servers.

`base_url` puts Helix on the mock's HTTP server; `ws_url` puts the EventSub socket on the
mock's WebSocket server, which only starts once an example has recorded messages, so until
then `ws_url` names an address nothing ever connects to.

The credentials are obviously fake and are handed to `Twitch.new` directly, which is what
keeps the token flow out of the tests: a client built with an `app_token` never calls
`https://id.twitch.tv/oauth2/token`, so nothing here touches the network. `token_url`
points at an unroutable address as a second guard, so a test that somehow reaches the
minting path fails to connect instead of quietly asking Twitch for a token.
"""

from pathlib import Path

import pytest
from truewire.mock import running_mock_servers

from twitch_helix import Twitch

PROJECT = Path(__file__).resolve().parents[1]

NO_WS_SERVER = 'ws://127.0.0.1:1/ws'
"""Stand-in for the mock's WebSocket address while no EventSub messages are recorded."""

UNREACHABLE_TOKEN_URL = 'http://127.0.0.1:1/oauth2/token'
"""Where a test that tried to mint a token would go, and fail, instead of to Twitch."""

CLIENT_ID = 'TEST_CLIENT_ID'
APP_TOKEN = 'TEST_APP_TOKEN'
USER_ACCESS_TOKEN = 'TEST_USER_ACCESS_TOKEN'


@pytest.fixture
def mock_servers():
  with running_mock_servers(PROJECT) as servers:
    yield servers


@pytest.fixture
def client(mock_servers):
  """The generated client, built exactly as a user would, against the mock's addresses."""
  ws_url = mock_servers.ws_server.url if mock_servers.ws_server is not None else NO_WS_SERVER
  return Twitch.new(
    base_url=mock_servers.http_base_url,
    ws_url=ws_url,
    client_id=CLIENT_ID,
    app_token=APP_TOKEN,
    user_access_token=USER_ACCESS_TOKEN,
    token_url=UNREACHABLE_TOKEN_URL,
    validate=True,
  )
