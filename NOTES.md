# Notes for the Truewire toolchain

Things Truewire 0.8.1 (`truewire-core` 0.2.1) could not express or do while this client was written, each with the exact error or warning where there was one and what the project does instead. Nothing here was worked around by bending the spec.

## 1. `truewire capture` records HTTP pairs only, so the EventSub socket cannot be recorded with it

`websocket.events` is the one `kind: "stream"` endpoint here, and `capture` refuses it before opening anything:

```text
websocket.events is not an HTTP rpc endpoint; capture records HTTP request/reply pairs only
  truewire/cli/capture.py:74
```

There is no other subcommand that records a subscription: `mock` replays one, `check` validates one, nothing captures one. So the project records the socket itself, in `test/record_events.py`: it resolves the endpoint's generated method the same way `capture` does (`truewire.examples.resolve_endpoint_function`), binds the recorded `examples/<id>.parameters.json` with `coerce_ws_example_call`, reads for a bounded window through the same generated client, and writes what arrived to `examples/<id>.messages.json` — the file `truewire check` validates and `truewire mock` replays. It builds the client with `validate=False`, because a recording has to be the wire body: validated, `message_timestamp` is already a `datetime` and no longer what Twitch sent.

A `capture` that accepted a stream endpoint with `--seconds`/`--limit` would replace that file exactly. The pieces it would need are all already public.

## 2. `capture` keeps the last exchange, so what a core does after the call would be recorded instead

`capture` wraps the whole call in `truewire_core.http.recording()`, which collects every exchange any `HttpClient` makes inside the block, and then takes one:

```python
exchange = exchanges[-1]
  truewire/cli/capture.py:95
```

This core makes two HTTP calls for the first endpoint call in a process: a `POST` to `https://id.twitch.tv/oauth2/token` for an app access token, then the Helix call. That is safe only because minting happens *before* the call, so the Helix exchange is last and the token exchange — whose response body holds a real access token — is discarded. A core that refreshed a token, reported a metric or released a lease *after* its request would silently record the wrong pair, and here that would mean writing a live credential into the tree.

Nothing warns about it. `capture` knows which endpoint it asked for and could select the exchange whose request matches that endpoint's method and path, rather than the last one to arrive.

## 3. A generated stream method has no `Literal[False]` overload for `validate`

Every rpc method overloads on `validate`, so `validate=False` returns `Any` and pyright knows the caller is holding the raw body. The stream method does not:

```python
client.websocket.events(validate=False)
# StreamManager[EventSubMessage, Any, Any]
```

At runtime that subscription yields raw frames — `metadata.message_timestamp` a `str`, not the `datetime` `MessageMetadata` declares — so the type is wrong in exactly the case the flag exists for. `test/typing_usage.py` asserts what the generator actually produces, with this note beside it, rather than asserting what it should produce. `test/record_events.py` is the one place the project relies on unvalidated messages, and it treats them as `Any`. The same pair of overloads the rpc path already emits would close it.

## 4. A union discriminated by a nested field is not usable, so five message kinds are one type

EventSub's socket carries five message shapes — `session_welcome`, `session_keepalive`, `notification`, `session_reconnect`, `revocation` — and `metadata.message_type` says which. The natural spec is an `anyOf` of five titled variants, each pinning `message_type` to its own single-value `enum`. That renders, and then does not work.

Written that way (tried, with two of the five variants, then reverted), pyright cannot narrow it, because the discriminant is one level down inside `metadata` rather than a key of the message itself:

```python
def f(message: EventSubMessage) -> None:
  if message['metadata']['message_type'] == 'session_welcome':
    reveal_type(message)
    # information: Type of "message" is "WelcomeMessage | KeepaliveMessage"
```

So every caller would carry a union it can never narrow, which is strictly worse than one `TypedDict` whose optional members say which kinds fill them in. The second cost is that the shared inner objects are emitted once per variant rather than once:

```text
MessagePayload0Payload, MessagePayload1Payload
Session0PayloadSession, Session1PayloadSession
```

One wire shape, two names with two variants, five with five. The bodies are identical.

The spec therefore declares one `EventSubMessage` with `metadata.message_type` as a `Literal` of the five values, and `payload` carrying `session`, `subscription` and `event` as optional members. The endpoint's `notes` and the README both say which kinds fill in which. Closing this needs two things that are not there: a way for a spec to name the discriminant (`anyOf` plus a `discriminator` path, which JSON Schema already has a word for), and a rendering that puts it somewhere a type checker can use — a `TypedDict` union narrows on its own key, so a variant would have to lift the discriminant, or the generator would have to emit tagged dataclasses instead.

## 5. `websocket.events` declares no `envelope.verb`, and cannot

`truewire check` warns, on every run:

```text
ADR 0004 — a `kind: "stream"` endpoint declares how its frames state subscribe/unsubscribe intent [warning]
   websocket.events  envelope.verb: this stream endpoint declares no `envelope.verb`, so the mock server cannot tell a subscribe frame from an unsubscribe frame for it without guessing -- declare `{"path": ..., "subscribe": ..., "unsubscribe": ...}` naming the field (and its two literal values) that states intent on this API's wire frame
```

There is nothing to declare. EventSub's WebSocket has no subscribe frame at all, and unusually it is not merely that nothing is sent — Twitch *closes the connection* on any client frame but a pong. A consumer opens `wss://eventsub.wss.twitch.tv/ws`, is welcomed with a session id, and the subscriptions are created over HTTP against that id. No frame ever travels up the socket, so any `envelope.verb` here would be invented, and inventing one to silence a check would put a lie in the spec and a matching lie in the mock. The endpoint declares `push: {trigger: connect}` instead, which is the honest statement of the same fact, and the mock serves it through that: `start_ws_server` pushes a `ConnectPush` example's messages the instant the connection is accepted, which is exactly what Twitch does. The client core says the same thing in code — `Connection.request_subscription` and `request_unsubscription` both send nothing.

The warning stands, unsilenced. The rule could exempt an endpoint that declares `push: connect`, since a stream with no outgoing frame has no verb to name.

## 6. Nothing can say that an `rpc` endpoint is what fills a `stream` endpoint

This is the shape that makes Twitch's EventSub interesting, and it is the one thing the spec format cannot state. `websocket.events` pushes nothing of consequence on its own; what arrives on it is decided by `eventsub.subscribe`, an ordinary `POST` whose `transport.session_id` is read out of the `session_welcome` message the socket sent. Two endpoints, two kinds, one dependency between them, and no field anywhere to write it in.

A spec can say a stream is push-on-connect (`push: {trigger: connect}`) or push-after-a-reply (`push: {trigger: after_rpc, method: ...}`), and neither covers this: the RPC is not a frame on this socket, it is a different endpoint on a different transport, and its input comes *from* the socket. So it is written in prose, three times — in the endpoint's `notes`, in the README, and here — which is exactly the "paragraph nobody can check" that per-endpoint declarations exist to replace. A third `push` trigger naming a sibling `rpc` endpoint and the field of the stream's own payload that feeds it would make it checkable, and would let the mock refuse a subscription created against a session id it never issued.

## 7. `truewire check` reads a bare string as a probable closed set

Two of the four standing rule-2 warnings are correct that the field *may* be a closed set and wrong that the set is knowable:

```text
2. Closed sets use `enum` [warnings]
   streams.get  responses.200.content.application/json.schema.properties.data.items.properties.type: `type` may be a closed set. Declare `enum` if the API documents its values; leave it bare if it does not — a guessed `enum` becomes a `Literal` that rejects values the API later sends
   websocket.events  responses.200.content.application/json.schema.properties.payload.properties.session.properties.status: `status` may be a closed set. Declare `enum` if the API documents its values; leave it bare if it does not — a guessed `enum` becomes a `Literal` that rejects values the API later sends
```

A `Stream`'s `type` is documented as `live`, and then documented as `""` when Twitch hits an error computing it: two values, of which the reference publishes one as a value and the other as prose. A session's `status` shows `connected` and `reconnecting` in the reference's own examples and is never given as a list. The other two warnings are on `eventsub.subscribe` and `eventsub.subscriptions`' request `type`, which is a subscription type name (`stream.online`, `channel.chat.message`) out of a table Twitch adds to every few weeks — the definition of a set that must not become a `Literal`.

An `enum` on any of them would render a `Literal` that raises on a value the API already sends. All four are left bare and the warnings stand. The rule has no way to say "open set, known values", which is the shape three of these four actually are.

The one closed set here that *is* published as a list — the `status` filter on `eventsub.subscriptions`, where an unlisted value is a `400` — does declare its `enum`. The same names appear a second time as the *response* field `status`, with two values the filter's list carries and the response's list does not; that field is bare, and its endpoint's `notes` say why.

## 8. A `description` beside a `$ref` never reaches the generated field

Authoring rule 7 requires a description on every response object property, `truewire check` accepts one written beside a `$ref`, and the generator drops it. The spec says:

```jsonc
"pagination": {
  "$ref": "Pagination",
  "description": "The cursor for the next page. Twitch sends this object empty once there are no more pages."
}
```

and the generated field has no docstring at all, while its plain sibling has one:

```python
class StreamsFrame(TypedDict):
  data: list[Stream]
  """The live broadcasts on this page, most watched first."""
  pagination: NotRequired[Pagination]
```

So the one field on every paged frame that a caller most needs explained is the one field with nothing on hover. This affects every `$ref`'d property in the project — `pagination` on four frames, `transport` on a subscription, `subscription` on a socket message. The description exists in the spec and satisfies the check; only the render loses it. Nothing here works around it: the prose lives in the endpoint's `notes` and the README instead.

## 9. There is no way to say "every response of this API is `{data: [...], pagination}`"

Helix has exactly one response shape. Five endpoints here declare it, and each one writes the wrapper out again, because a JSON Schema `$ref` cannot take the item type as a parameter:

```text
UsersFrame, StreamsFrame, VideosFrame, ClipsFrame, GamesFrame, TopGamesFrame,
ChannelsFrame, SubscriptionsFrame, CreateSubscriptionFrame
```

Nine near-identical two-property objects, differing in the element type of `data`. `truewire standards --only duplicate-schemas` does not flag them, and it is right not to — they are distinct declarations, not a shared shape someone forgot to `$ref` — but the repetition is real, and the next endpoint added to this client will copy it again. A generic named schema (`Frame<T>`, or a `$ref` with an argument) would collapse all nine, and would let the generator emit one `Frame[Stream]` alias per endpoint instead of one class.

Keeping the frame is itself the right call here and not a workaround: unwrapping `data` would put the cursor out of reach of the walk, since pagination paths resolve inside whatever the method returns, and the EventSub frames carry `total`, `total_cost` and `max_total_cost` beside `data` (authoring rule 6's own "keep the envelope when it carries something the caller needs", ADR 0010).

## 10. Mutually exclusive request parameters cannot be stated

`videos.get` takes exactly one of `id`, `user_id` and `game_id`; `clips.get` takes exactly one of `id`, `broadcaster_id` and `game_id`; `eventsub.subscriptions` takes at most one of its five filters. Twitch documents all three constraints and answers `400` when they are broken. Nothing in an `endpoint.json` can say it, so all three render as independent optional keyword arguments and a caller who passes two finds out from the API.

Authoring rule 0 has the shape for the body case — an `anyOf` of titled variants, one per valid combination — but these are query parameters, and a request `anyOf` renders as a single union-typed parameter, which is the wrong call ergonomics for a `GET` with eight other filters beside the exclusive three. The constraint is in each endpoint's request-property descriptions instead, where only a human reads it.

## 11. Generated import order does not satisfy ruff's isort rule

The generator writes `from truewire_core...`, `from typing_extensions...`, then first-party imports without the blank line and ordering ruff's `I001` wants (its shipped `resources/ruff.toml` selects only `TID251`):

```text
I001 [*] Import block is un-sorted or un-formatted
 --> src/twitch_helix/channels/get.py:2:1
  |
1 |   # Generated by truewire — do not edit by hand.
2 | / from typing_extensions import Any, Literal, NotRequired, TypedDict, overload
3 | | from twitch_helix.core import Endpoint
  | |______________________________________^
```

A project that lints with `I` needs per-file ignores for the generated modules, as `ruff.toml` here has. Emitting isort-sorted imports would remove that.

## 12. The scaffolded package `__init__.py` fails a project that lints with `F`

`truewire init` writes `src/<package>/__init__.py` as a bare re-export, which ruff reports the moment `F` is selected:

```text
F401 `.main.Twitch` imported but unused; consider removing, adding to `__all__`, or using a redundant alias
 --> src/twitch_helix/__init__.py:1:19
```

The file is not in the generator's manifest, so it is the project's to fix, and this one gives it a module docstring and an `__all__`. Small, but it is the first thing a new project's own lint gate says about code the toolchain wrote. `init` writing `__all__ = ['<Class>']` would fix it once for everyone.
