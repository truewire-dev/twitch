"""Real, representative usage of the client's public surface, type-checked by pyright
(`pyrightconfig.json`) and never executed: the guardrail against a public return type
silently degrading, and the proof that `validate=False` is typed as what it returns.

`reveal_type(..., expected_text=...)` is pyright's own assertion: a mismatch is an error.
"""

from typing_extensions import reveal_type

from twitch_helix import Twitch


async def the_point_lookups() -> None:
  """Every Helix method returns the wire frame, and the rows are `data`."""
  async with Twitch.new() as client:
    users = await client.users.get(login=['twitchdev'])
    reveal_type(users, expected_text='UsersFrame')
    reveal_type(users['data'], expected_text='list[User]')
    reveal_type(users['data'][0]['display_name'], expected_text='str')
    reveal_type(users['data'][0]['created_at'], expected_text='datetime')
    reveal_type(
      users['data'][0]['broadcaster_type'],
      expected_text="Literal['', 'affiliate', 'partner']",
    )
    reveal_type(users['data'][0].get('email'), expected_text='str | None')
    channels = await client.channels.get(broadcaster_id=['141981764'])
    reveal_type(channels['data'][0]['title'], expected_text='str')
    reveal_type(channels['data'][0].get('tags'), expected_text='list[str] | None')
    games = await client.games.get(name=['Science & Technology'])
    reveal_type(games['data'][0], expected_text='Game')


async def the_lists(live: bool) -> None:
  """A closed set in the docs is a `Literal` at the call site, and a nullable field is `| None`."""
  async with Twitch.new() as client:
    streams = await client.streams.get(user_login=['twitchdev'], type='live' if live else 'all')
    reveal_type(streams, expected_text='StreamsFrame')
    reveal_type(streams['data'][0]['viewer_count'], expected_text='int')
    reveal_type(streams['data'][0]['started_at'], expected_text='datetime')
    reveal_type(streams.get('pagination'), expected_text='Pagination | None')
    videos = await client.videos.get(user_id='141981764', sort='time', type='archive')
    reveal_type(
      videos['data'][0]['type'], expected_text="Literal['archive', 'highlight', 'upload']"
    )
    reveal_type(videos['data'][0].get('stream_id'), expected_text='str | None')
    reveal_type(videos['data'][0].get('muted_segments'), expected_text='list[MutedSegment] | None')
    clips = await client.clips.get(broadcaster_id='141981764', first=5)
    reveal_type(clips['data'][0]['duration'], expected_text='float')
    reveal_type(clips['data'][0].get('vod_offset'), expected_text='int | None')


async def the_cursor_walk() -> None:
  """A `_paged` walk: awaited flat, iterated a page at a time, or resumed from a state."""
  async with Twitch.new() as client:
    walk = client.games.top_paged(first=100)
    reveal_type(walk, expected_text='PaginatedResponse[Game, str]')
    reveal_type(await walk, expected_text='Sequence[Game]')
    async for page in walk:
      reveal_type(page, expected_text='Sequence[Game]')
      reveal_type(page[0]['name'], expected_text='str')
    # The cursor itself: each page carries the state before it and the state after it,
    # `None` once Twitch answers with an empty `pagination`, and `resume` restarts from
    # a saved one.
    async for checkpoint in walk.pages():
      reveal_type(checkpoint, expected_text='Page[Game, str]')
      reveal_type(checkpoint.state, expected_text='str')
      reveal_type(checkpoint.next, expected_text='str | None')
      if checkpoint.next is not None:
        reveal_type(walk.resume(checkpoint.next), expected_text='PaginatedResponse[Game, str]')
    reveal_type(
      client.streams.get_paged(game_id=['509670']),
      expected_text='PaginatedResponse[Stream, str]',
    )
    reveal_type(
      client.clips.get_paged(broadcaster_id='141981764'),
      expected_text='PaginatedResponse[Clip, str]',
    )
    reveal_type(
      client.eventsub.subscriptions_paged(status='enabled'),
      expected_text='PaginatedResponse[EventSubSubscription, str]',
    )


async def the_eventsub_pair(session_id: str) -> None:
  """The two halves of EventSub: a subscription over HTTP, the events over the socket."""
  async with Twitch.new(user_access_token='...') as client:
    created = await client.eventsub.subscribe(
      type='stream.online',
      version='1',
      condition={'broadcaster_user_id': '141981764'},
      transport={'method': 'websocket', 'session_id': session_id},
    )
    reveal_type(created, expected_text='CreateSubscriptionFrame')
    reveal_type(created['data'][0]['id'], expected_text='str')
    reveal_type(
      created['data'][0]['transport']['method'],
      expected_text="Literal['webhook', 'websocket', 'conduit']",
    )
    reveal_type(created['data'][0]['condition'], expected_text='dict[str, str]')
    reveal_type(created.get('max_total_cost'), expected_text='int | None')

    subscription = client.websocket.events(keepalive_timeout_seconds=30)
    reveal_type(subscription, expected_text='StreamManager[EventSubMessage, Any, Any]')
    async with subscription as messages:
      async for message in messages:
        reveal_type(message, expected_text='EventSubMessage')
        reveal_type(
          message['metadata']['message_type'],
          expected_text=(
            "Literal['session_welcome', 'session_keepalive', 'notification', "
            "'session_reconnect', 'revocation']"
          ),
        )
        reveal_type(message['metadata']['message_timestamp'], expected_text='datetime')
        # `message_type` is the discriminator, but it sits one level down, inside
        # `metadata`, and a nested literal narrows nothing: whichever shape the message
        # is, `payload` is one optional type and a caller reads through `.get`. NOTES.md 2.
        payload = message.get('payload')
        reveal_type(payload, expected_text='MessagePayload | None')
        if payload is None:
          continue
        reveal_type(payload.get('session'), expected_text='Session | None')
        reveal_type(payload.get('subscription'), expected_text='EventSubSubscription | None')
        reveal_type(payload.get('event'), expected_text='dict[str, Any] | None')
        session = payload.get('session')
        if session is not None:
          reveal_type(session['id'], expected_text='str')
          reveal_type(session.get('reconnect_url'), expected_text='str | None')


async def raw_bodies() -> None:
  """`validate=False` returns the body as the wire sent it, and says so: `Any`."""
  async with Twitch.new() as client:
    reveal_type(await client.users.get(login=['twitchdev'], validate=False), expected_text='Any')
    reveal_type(await client.games.top(first=3, validate=False), expected_text='Any')
    # A walk carries the same flag down to each page, and its row type with it.
    reveal_type(
      client.games.top_paged(first=100, validate=False),
      expected_text='PaginatedResponse[Any, str]',
    )
    # The stream is the exception: it has no `Literal[False]` overload, so an unvalidated
    # subscription still reads as `EventSubMessage` while it yields raw frames, where
    # `message_timestamp` is the string Twitch sent rather than a `datetime`. NOTES.md 1.
    reveal_type(
      client.websocket.events(validate=False),
      expected_text='StreamManager[EventSubMessage, Any, Any]',
    )
