"""Hand-written core for the Twitch client: the app access token, the two headers every
Helix call carries, the error mapping, and the base classes the generated endpoints
subclass.

Nothing here is generated, and regenerating the client never touches it. Generated `rpc`
endpoints subclass `Endpoint` and call `self.request(...)`; the generated EventSub
endpoint subclasses `StreamEndpoint` and calls `self.subscribe(...)`, which opens one
WebSocket per subscription through `.ws.SocketClient`.

Helix authenticates every call, public data included. An application registered at
https://dev.twitch.tv/console/apps has a client id and a client secret; the client
credentials flow at `https://id.twitch.tv/oauth2/token` exchanges the pair for an *app
access token*, which is what almost every endpoint here wants. The token is minted on
the first call that needs one, cached until shortly before it expires, and re-minted
once when the API answers `401`.

The credentials reach this core one of two ways: passed to `Twitch.new(client_id=...,
client_secret=...)`, or read from `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET` in the
environment when they were not. Neither the credentials nor the token they mint are ever
written anywhere: `Secret` keeps them out of every repr and every log line, and both
travel in headers, which no recorded example holds.

Creating an EventSub subscription is the one endpoint here that needs a *user* access
token, which the client credentials flow cannot mint. Those endpoints declare
`meta: {"token": "user"}`, and a client built without `user_access_token=` refuses them
here, before any request is made.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import UnionType

from typing_extensions import Any, Literal, Self, TypeVar, cast

from truewire_core.exceptions import ApiError, AuthError, BadRequest, RateLimited
from truewire_core.http import HttpClient
from truewire_core.util import StreamManager
from truewire_core.validation import validator

from ..meta import HelixMeta as Meta
from .ws import SocketClient

T = TypeVar('T')

HELIX = 'https://api.twitch.tv'
"""The Helix host. Every path in this spec starts `/helix/`."""

OAUTH_TOKEN = 'https://id.twitch.tv/oauth2/token'
"""Twitch's OAuth token endpoint, on the identity host rather than the API host."""

EVENTSUB = 'wss://eventsub.wss.twitch.tv/ws'
"""The EventSub WebSocket. A `session_reconnect` message names a different URL to move
to, which is why the caller can pass one to `Twitch.new(ws_url=...)`."""

CLIENT_ID_VAR = 'TWITCH_CLIENT_ID'
CLIENT_SECRET_VAR = 'TWITCH_CLIENT_SECRET'
"""The two environment variables the core falls back to. Names only: no value of either
is ever written into this repository, and `truewire standards` reads these same two names
out of `truewire.toml` to flag a recording that leaked one."""

REFRESH_MARGIN = timedelta(seconds=60)
"""How long before its stated expiry a cached app access token is re-minted, so a call
never leaves with a token that expires in flight."""

Tier = Literal['app', 'user']
"""Which credential tier an endpoint needs, from its `meta.token` (authoring rule 9)."""


@dataclass(frozen=True)
class Secret:
  """A credential that never renders itself.

  A client secret, an app access token and a user access token are all this. `repr` and
  `str` are the two ways a value leaks into a traceback, a log line or a captured
  example, so both are the same six characters, and reading the value is an explicit
  `reveal()` at the one place that puts it on the wire.
  """

  value: str

  def reveal(self) -> str:
    """The value itself, for the header that carries it."""
    return self.value

  def __repr__(self) -> str:
    return "Secret('******')"

  def __str__(self) -> str:
    return '******'


def from_environment(name: str, *, needed_for: str) -> str:
  """Read one credential out of the environment, or raise `AuthError` saying which.

  Never a bare `os.environ[...]`: a missing credential is an auth failure with a name in
  it, not a `KeyError` two frames deeper (production standard S9).
  """
  value = os.environ.get(name)
  if not value:
    raise AuthError(
      f'{name} is not set, and {needed_for}. Export it, or pass it to Twitch.new() '
      f'({CLIENT_ID_VAR} is client_id=, {CLIENT_SECRET_VAR} is client_secret=).'
    )
  return value


def now() -> datetime:
  return datetime.now(timezone.utc)


@dataclass(kw_only=True)
class Auth:
  """The credentials, the app access token minted from them, and their expiry.

  One instance per client. `token()` is the only entry point: it returns the token for
  the tier an endpoint asked for, minting and caching the app one as needed, and
  serialising concurrent callers on `lock` so a burst of parallel calls mints once.
  """

  http: HttpClient
  client_id: str | None = None
  client_secret: Secret | None = None
  app_token: Secret | None = None
  user_access_token: Secret | None = None
  token_url: str = OAUTH_TOKEN
  expires_at: datetime | None = None
  lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

  def identity(self) -> str:
    """The client id, which every Helix call carries as `Client-Id` whatever the tier."""
    if self.client_id is None:
      self.client_id = from_environment(
        CLIENT_ID_VAR, needed_for='every Helix call carries it as the `Client-Id` header'
      )
    return self.client_id

  def secret(self) -> Secret:
    """The client secret, read from the environment the first time it is needed."""
    if self.client_secret is None:
      self.client_secret = Secret(
        from_environment(
          CLIENT_SECRET_VAR,
          needed_for='an app access token is minted from the client id and secret',
        )
      )
    return self.client_secret

  def stale(self) -> bool:
    """Whether the cached app token is gone or close enough to its expiry to re-mint.

    A token handed to `Twitch.new(app_token=...)` has no stated expiry, so it is never
    stale by the clock; a `401` is the only thing that retires it.
    """
    if self.app_token is None:
      return True
    return self.expires_at is not None and now() >= self.expires_at - REFRESH_MARGIN

  async def mint(self) -> Secret:
    """Exchange the client id and secret for an app access token.

    The client credentials grant: a form-encoded `POST` to the identity host, answered
    with `{"access_token": ..., "expires_in": ..., "token_type": "bearer"}`. Roughly 60
    days of validity in practice, but `expires_in` is what is trusted.
    """
    response = await self.http.request(
      'POST',
      self.token_url,
      data={
        'client_id': self.identity(),
        'client_secret': self.secret().reveal(),
        'grant_type': 'client_credentials',
      },
      headers={'Accept': 'application/json'},
    )
    if response.status_code >= 400:
      raise AuthError(
        f'POST {self.token_url}: HTTP {response.status_code}: could not mint an app '
        f'access token from {CLIENT_ID_VAR}/{CLIENT_SECRET_VAR}: {response.text[:200]}'
      )
    body = response.json()
    token = body.get('access_token')
    if not isinstance(token, str):
      raise AuthError(f'POST {self.token_url}: no `access_token` in the reply')
    self.app_token = Secret(token)
    seconds = body.get('expires_in')
    self.expires_at = now() + timedelta(seconds=seconds) if isinstance(seconds, int) else None
    return self.app_token

  async def token(self, tier: Tier) -> Secret:
    """The bearer token for one call, by the tier its endpoint declared.

    Raises:
      AuthError: When a `user` endpoint is called on a client holding no user access
        token. The client credentials flow cannot mint one: a user token comes from an
        authorization code flow a person completes in a browser.
    """
    if tier == 'user':
      if self.user_access_token is None:
        raise AuthError(
          'this endpoint needs a user access token with the scopes it documents; the '
          'client credentials flow cannot mint one. Pass Twitch.new(user_access_token=...) '
          'with a token from an authorization code grant.'
        )
      return self.user_access_token
    async with self.lock:
      if self.stale():
        return await self.mint()
      return cast(Secret, self.app_token)

  def invalidate(self, tier: Tier) -> bool:
    """Drop the cached app token after a `401`; whether a retry can mint a new one."""
    if tier != 'app':
      return False
    self.app_token = None
    self.expires_at = None
    return self.client_secret is not None or bool(os.environ.get(CLIENT_SECRET_VAR))


def retry_after(headers: Any) -> str:
  """How long the rate limit lasts, read off `Ratelimit-Reset`.

  Helix answers `429` with a `Ratelimit-Reset` header holding the Unix second the bucket
  refills at, beside `Ratelimit-Limit` and `Ratelimit-Remaining` (which is `0`). The
  header is the only place the wait is stated; the body says nothing about it.
  """
  reset = headers.get('Ratelimit-Reset')
  try:
    at = datetime.fromtimestamp(int(reset), timezone.utc)
  except (TypeError, ValueError):
    return 'no Ratelimit-Reset header'
  seconds = max(0.0, (at - now()).total_seconds())
  return f'the bucket refills at {at.isoformat()}, in {seconds:.0f}s'


def raise_for_status(method: str, path: str, status: int, text: str, headers: Any) -> None:
  """Map a non-2xx Helix answer onto the runtime's exceptions.

  A Helix error body is `{"error": "Unauthorized", "status": 401, "message": "..."}`;
  `error` and `message` both carry information, so both go into the exception. `400` and
  `404` are bad requests, `401`/`403` auth failures, `429` the rate limit with its reset
  time; everything else is an `ApiError`.
  """
  reason = text[:200]
  try:
    body = json.loads(text)
  except ValueError:
    body = None
  if isinstance(body, dict):
    parts = [str(body[key]) for key in ('error', 'message') if isinstance(body.get(key), str)]
    if parts:
      reason = ': '.join(parts)
  message = f'{method} {path}: HTTP {status}: {reason}'
  if status in (400, 404, 422):
    raise BadRequest(message)
  if status in (401, 403):
    raise AuthError(message)
  if status == 429:
    raise RateLimited(f'{message} ({retry_after(headers)})')
  raise ApiError(message)


def render(request: Any, request_type: type[Any] | UnionType | None) -> dict[str, Any]:
  """The request's fields in wire form, without the ones left unset.

  Rendering through the request's own type applies every declared wire format, so a
  `started_at` passed as a `datetime` reaches the query string as the RFC 3339 string
  Helix reads, never `str()` of a `datetime`.
  """
  if request is None:
    return {}
  if request_type is None:
    rendered = dict(request)
  else:
    rendered = json.loads(validator(cast(type, request_type)).dump(request))
  return {k: v for k, v in rendered.items() if v is not None}


@dataclass(kw_only=True)
class Transport:
  """The HTTP transport: one host, one connection pool, one set of credentials."""

  base_url: str
  auth: Auth
  http: HttpClient
  validate: bool = True

  async def headers(self, *, tier: Tier) -> dict[str, str]:
    """The two headers every Helix call carries, plus the media type.

    `Client-Id` is the application, `Authorization` the token; Helix refuses a call
    missing either, whatever the endpoint reads.
    """
    token = await self.auth.token(tier)
    return {
      'Accept': 'application/json',
      'Client-Id': self.auth.identity(),
      'Authorization': f'Bearer {token.reveal()}',
    }

  async def send(
    self, method: str, path: str, *, params: dict[str, Any], body: bytes | None, tier: Tier
  ) -> bytes:
    """Send one request and return the body; a non-2xx status raises.

    A `401` on an app-token call is the token having expired early or been revoked, so
    the cached one is dropped and the call is made once more with a freshly minted one.
    A second `401` is a real auth failure and raises. Every other status goes straight
    to `raise_for_status`.
    """
    filled = path
    for name, value in list(params.items()):
      if f'{{{name}}}' in filled:
        filled = filled.replace(f'{{{name}}}', str(value))
        params.pop(name)
    url = self.base_url.rstrip('/') + '/' + filled.lstrip('/')
    headers = {
      **await self.headers(tier=tier),
      **({'Content-Type': 'application/json'} if body else {}),
    }
    response = await self.http.request(
      method, url, params=params or None, content=body, headers=headers
    )
    if response.status_code == 401 and self.auth.invalidate(tier):
      response = await self.http.request(
        method,
        url,
        params=params or None,
        content=body,
        headers={
          **await self.headers(tier=tier),
          **({'Content-Type': 'application/json'} if body else {}),
        },
      )
    if response.status_code >= 400:
      raise_for_status(method, filled, response.status_code, response.text, response.headers)
    return response.content


@dataclass(kw_only=True)
class ClientBase:
  """Root client: the HTTP transport every Helix group shares, and the EventSub socket."""

  client: Transport
  socket: SocketClient

  @classmethod
  def new(
    cls,
    *,
    base_url: str = HELIX,
    ws_url: str = EVENTSUB,
    client_id: str | None = None,
    client_secret: str | None = None,
    app_token: str | None = None,
    user_access_token: str | None = None,
    token_url: str = OAUTH_TOKEN,
    validate: bool = True,
  ) -> Self:
    """Create a client.

    Credentials are optional here and read from `TWITCH_CLIENT_ID` and
    `TWITCH_CLIENT_SECRET` when the first call needs them, so a client built with no
    arguments works wherever those are exported and fails with a named `AuthError`
    where they are not.

    Args:
      base_url: The Helix host. A `truewire mock` address in tests.
      ws_url: The EventSub WebSocket. Pass the `reconnect_url` from a `session_reconnect`
        message to move a subscription to the socket Twitch is asking for.
      client_id: The application's client id; defaults to `TWITCH_CLIENT_ID`.
      client_secret: The application's client secret; defaults to `TWITCH_CLIENT_SECRET`.
        Only ever used to mint an app access token.
      app_token: An app access token you already hold, used as-is instead of minting one.
        A client built with one needs no client secret, only the client id.
      user_access_token: A user access token from an authorization code grant, for the
        endpoints that declare `meta: {"token": "user"}`. The client credentials flow
        cannot mint one.
      token_url: Twitch's OAuth token endpoint; a local address in tests.
      validate: Whether responses and pushed messages are validated against their
        declared types by default. A call's own `validate=` overrides it.
    """
    http = HttpClient()
    auth = Auth(
      http=http,
      client_id=client_id,
      client_secret=Secret(client_secret) if client_secret is not None else None,
      app_token=Secret(app_token) if app_token is not None else None,
      user_access_token=Secret(user_access_token) if user_access_token is not None else None,
      token_url=token_url,
    )
    return cls(
      client=Transport(base_url=base_url, auth=auth, http=http, validate=validate),
      socket=SocketClient.new(ws_url, validate=validate),
    )

  async def __aenter__(self) -> Self:
    return self

  async def __aexit__(self, exc_type, exc_value, traceback):
    await self.socket.__aexit__(exc_type, exc_value, traceback)
    await self.client.http.__aexit__(exc_type, exc_value, traceback)


@dataclass(kw_only=True, frozen=True)
class Endpoint:
  """Base for every generated Helix endpoint class: the shared HTTP transport."""

  client: Transport

  async def request(
    self,
    request: Any = None,
    *,
    method: str,
    path: str,
    validate: bool | None = None,
    request_type: type[Any] | UnionType | None = None,
    response_type: type[T] | UnionType | None = None,
    meta: Meta,
  ) -> T:
    """Send one request and validate the reply against `response_type`.

    A Helix `GET` takes every parameter in the query string, a list-valued one (`id`,
    `login`, `broadcaster_id`) as repeated keys, which is how `httpx` renders a list and
    what Helix reads. A `POST` sends the request as a JSON body instead, serialised
    through its own type so declared wire formats apply (production standard S28).
    """
    params = render(request, request_type)
    body = None
    if method.upper() in ('POST', 'PUT', 'PATCH'):
      if request is not None and request_type is not None:
        body = validator(cast(type, request_type)).dump(request)
      params = {}
    raw = await self.client.send(method, path, params=params, body=body, tier=meta['token'])
    if response_type is None:
      return None  # type: ignore[return-value]
    check = self.client.validate if validate is None else validate
    if check:
      return validator(cast(type, response_type)).json(raw)
    return json.loads(raw)


@dataclass(kw_only=True, frozen=True)
class StreamEndpoint:
  """Base for the generated EventSub endpoint class: the socket client."""

  client: SocketClient

  def subscribe(
    self,
    channel: str,
    parameters: Any = None,
    *,
    validate: bool | None = None,
    request_type: type[Any] | UnionType | None = None,
    response_type: type[T] | UnionType | None = None,
  ) -> StreamManager[T, Any, Any]:
    """Open one EventSub socket; each pushed message validates against `response_type`.

    EventSub's WebSocket has no subscribe frame: the connection's own parameters travel
    in its URL, the welcome message hands back a session id, and the subscriptions
    themselves are created over HTTP against that id. `channel` only names the
    subscription locally.
    """
    params = render(parameters, request_type)
    payload_validator = validator(cast(type, response_type)) if response_type is not None else None
    return self.client.subscribe(
      channel, params, payload_validator=payload_validator, validate=validate
    )
