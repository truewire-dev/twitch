"""The hand-written core, without the network: hosts, credentials, the query it sends,
the token flow, and the error mapping.

Nothing here reaches api.twitch.tv, id.twitch.tv or eventsub.wss.twitch.tv: the HTTP
calls go through an in-memory `httpx` transport that records the request the core built,
and the socket side is checked at the URL its subscription would open.
"""

import json

import httpx
import pytest
from truewire_core.exceptions import ApiError, AuthError, BadRequest, RateLimited

from twitch_helix import Twitch
from twitch_helix.core import (
  CLIENT_ID_VAR,
  EVENTSUB,
  HELIX,
  OAUTH_TOKEN,
  Secret,
  raise_for_status,
  retry_after,
)
from twitch_helix.core.ws import connection_url


def test_the_defaults_are_the_real_hosts():
  client = Twitch.new(client_id='ID', app_token='TOKEN')
  assert client.client.base_url == HELIX
  assert client.socket.url == EVENTSUB
  assert client.client.auth.token_url == OAUTH_TOKEN


def test_a_secret_never_renders_itself():
  """The one guard between a credential and a traceback, a log line or a recording."""
  secret = Secret('super-secret-value')
  assert 'super-secret-value' not in repr(secret)
  assert 'super-secret-value' not in str(secret)
  assert 'super-secret-value' not in f'{secret}'
  assert 'super-secret-value' not in repr(Twitch.new(client_secret='super-secret-value'))
  assert secret.reveal() == 'super-secret-value', 'and reading it is explicit'


def test_a_missing_client_id_is_an_auth_error_naming_the_variable(monkeypatch):
  """Not a `KeyError` from a bare environment lookup (production standard S9)."""
  monkeypatch.delenv(CLIENT_ID_VAR, raising=False)
  client = Twitch.new()
  with pytest.raises(AuthError, match=CLIENT_ID_VAR):
    client.client.auth.identity()


def test_credentials_fall_back_to_the_environment(monkeypatch):
  monkeypatch.setenv(CLIENT_ID_VAR, 'ID_FROM_THE_ENVIRONMENT')
  assert Twitch.new().client.auth.identity() == 'ID_FROM_THE_ENVIRONMENT'
  assert Twitch.new(client_id='EXPLICIT').client.auth.identity() == 'EXPLICIT'


@pytest.mark.asyncio
async def test_a_user_tier_endpoint_without_a_user_token_is_refused_before_the_request():
  """`meta: {"token": "user"}` is the one per-endpoint fact the core reads."""
  client = Twitch.new(client_id='ID', app_token='TOKEN')
  seen: list[httpx.Request] = []
  capturing(client, seen, json_body={'data': []})
  async with client:
    with pytest.raises(AuthError, match='user access token'):
      await client.eventsub.subscribe(
        type='stream.online',
        version='1',
        condition={'broadcaster_user_id': '141981764'},
        transport={'method': 'websocket', 'session_id': 'SESSION'},
      )
  assert seen == [], 'nothing left for the wire'


def capturing(client: Twitch, seen: list[httpx.Request], *, json_body, status: int = 200) -> None:
  """Route the transport's HTTP through an in-memory handler that records the request."""

  def handler(request: httpx.Request) -> httpx.Response:
    seen.append(request)
    if request.url.path.endswith('/oauth2/token'):
      return httpx.Response(
        200, json={'access_token': 'MINTED', 'expires_in': 5011271, 'token_type': 'bearer'}
      )
    return httpx.Response(status, json=json_body)

  client.client.http._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_a_helix_call_carries_both_headers_and_puts_everything_in_the_query():
  seen: list[httpx.Request] = []
  client = Twitch.new(client_id='ID', app_token='TOKEN')
  capturing(client, seen, json_body={'data': []})
  async with client:
    await client.streams.get(user_login=['twitchdev'], first=3, validate=False)
  [request] = seen
  assert request.method == 'GET'
  assert request.url.host == 'api.twitch.tv'
  assert request.url.path == '/helix/streams'
  assert request.url.params['user_login'] == 'twitchdev'
  assert request.url.params['first'] == '3'
  assert 'after' not in request.url.params, 'an omitted parameter is not sent'
  assert request.headers['Client-Id'] == 'ID'
  assert request.headers['Authorization'] == 'Bearer TOKEN'


@pytest.mark.asyncio
async def test_a_list_parameter_travels_as_repeated_keys():
  seen: list[httpx.Request] = []
  client = Twitch.new(client_id='ID', app_token='TOKEN')
  capturing(client, seen, json_body={'data': []})
  async with client:
    await client.users.get(login=['twitchdev', 'twitch'], validate=False)
  [request] = seen
  assert request.url.params.get_list('login') == ['twitchdev', 'twitch']


@pytest.mark.asyncio
async def test_a_post_sends_a_json_body():
  seen: list[httpx.Request] = []
  client = Twitch.new(client_id='ID', app_token='TOKEN', user_access_token='USER')
  capturing(client, seen, json_body={'data': [], 'total': 0}, status=202)
  async with client:
    await client.eventsub.subscribe(
      type='stream.online',
      version='1',
      condition={'broadcaster_user_id': '141981764'},
      transport={'method': 'websocket', 'session_id': 'SESSION'},
      validate=False,
    )
  [request] = seen
  assert request.method == 'POST'
  assert request.url.query == b'', 'the request is the body, not the query string'
  assert json.loads(request.content) == {
    'type': 'stream.online',
    'version': '1',
    'condition': {'broadcaster_user_id': '141981764'},
    'transport': {'method': 'websocket', 'session_id': 'SESSION'},
  }
  assert request.headers['Authorization'] == 'Bearer USER', 'the user tier, not the app one'


@pytest.mark.asyncio
async def test_the_app_token_is_minted_once_and_reused():
  """The client credentials flow: one `POST` to the identity host, then a cached token."""
  seen: list[httpx.Request] = []
  client = Twitch.new(client_id='ID', client_secret='SECRET')
  capturing(client, seen, json_body={'data': []})
  async with client:
    await client.games.top(first=3, validate=False)
    await client.games.top(first=3, validate=False)
  minting = [r for r in seen if r.url.path.endswith('/oauth2/token')]
  assert len(minting) == 1, 'the second call reuses the cached token'
  assert dict(httpx.QueryParams(minting[0].content.decode())) == {
    'client_id': 'ID',
    'client_secret': 'SECRET',
    'grant_type': 'client_credentials',
  }
  assert [r.headers['Authorization'] for r in seen if r.url.host == 'api.twitch.tv'] == [
    'Bearer MINTED',
    'Bearer MINTED',
  ]


@pytest.mark.asyncio
async def test_a_401_drops_the_cached_token_and_retries_once():
  """A token can be revoked before it expires, and one re-auth is the whole recovery."""
  seen: list[httpx.Request] = []
  client = Twitch.new(client_id='ID', client_secret='SECRET', app_token='STALE')

  def handler(request: httpx.Request) -> httpx.Response:
    seen.append(request)
    if request.url.path.endswith('/oauth2/token'):
      return httpx.Response(200, json={'access_token': 'FRESH', 'expires_in': 5011271})
    if request.headers['Authorization'] == 'Bearer STALE':
      return httpx.Response(
        401, json={'error': 'Unauthorized', 'status': 401, 'message': 'Invalid OAuth token'}
      )
    return httpx.Response(200, json={'data': []})

  client.client.http._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
  async with client:
    await client.games.top(validate=False)
  assert [r.url.path for r in seen] == ['/helix/games/top', '/oauth2/token', '/helix/games/top']
  assert seen[-1].headers['Authorization'] == 'Bearer FRESH'


@pytest.mark.asyncio
async def test_a_second_401_is_an_auth_error():
  seen: list[httpx.Request] = []
  client = Twitch.new(client_id='ID', client_secret='SECRET', app_token='STALE')

  def handler(request: httpx.Request) -> httpx.Response:
    seen.append(request)
    if request.url.path.endswith('/oauth2/token'):
      return httpx.Response(200, json={'access_token': 'ALSO_REFUSED', 'expires_in': 100})
    return httpx.Response(401, json={'error': 'Unauthorized', 'message': 'Invalid OAuth token'})

  client.client.http._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
  async with client:
    with pytest.raises(AuthError, match='Unauthorized: Invalid OAuth token'):
      await client.games.top(validate=False)
  assert [r.url.path for r in seen].count('/helix/games/top') == 2, 'retried once, not forever'


def test_errors_map_onto_the_runtime_exceptions():
  helix = '{"error": "Bad Request", "status": 400, "message": "Malformed query params."}'
  with pytest.raises(BadRequest, match='Bad Request: Malformed query params.'):
    raise_for_status('GET', '/helix/streams', 400, helix, httpx.Headers())
  with pytest.raises(AuthError):
    raise_for_status('GET', '/helix/streams', 401, '{"error": "Unauthorized"}', httpx.Headers())
  with pytest.raises(RateLimited, match='refills at'):
    raise_for_status(
      'GET',
      '/helix/streams',
      429,
      '{"error": "Too Many Requests"}',
      httpx.Headers(
        {'Ratelimit-Limit': '800', 'Ratelimit-Remaining': '0', 'Ratelimit-Reset': '4102444800'}
      ),
    )
  with pytest.raises(ApiError, match='HTTP 502: <html>'):
    raise_for_status('GET', '/helix/streams', 502, '<html>bad gateway</html>', httpx.Headers())


def test_the_rate_limit_wait_comes_off_the_header():
  """`Ratelimit-Reset` is a Unix second, and the only place the wait is stated."""
  assert '2100-01-01' in retry_after(httpx.Headers({'Ratelimit-Reset': '4102444800'}))
  assert retry_after(httpx.Headers()) == 'no Ratelimit-Reset header'
  assert retry_after(httpx.Headers({'Ratelimit-Reset': 'soon'})) == 'no Ratelimit-Reset header'


def test_a_subscription_carries_its_parameters_in_the_connection_url():
  """EventSub has no subscribe frame; the socket's settings are its query string."""
  assert connection_url(EVENTSUB, {}) == EVENTSUB
  assert (
    connection_url(EVENTSUB, {'keepalive_timeout_seconds': 30})
    == 'wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds=30'
  )
  assert connection_url(EVENTSUB, {'keepalive_timeout_seconds': None}) == EVENTSUB
  reconnect = 'wss://eventsub.wss.twitch.tv/ws?challenge=abc&id=xyz'
  assert connection_url(reconnect, {'keepalive_timeout_seconds': 30}).startswith(reconnect + '&'), (
    'a reconnect_url already carries a query string'
  )
