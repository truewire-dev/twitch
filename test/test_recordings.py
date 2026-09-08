"""What each recording proves about the typed result, beyond validating.

One test per example request. An example whose response has not been recorded yet skips
with the reason, so the suite is green before the first run of `test/recapture.sh` and
turns into real coverage the moment the recordings land. The assertions are structural
(the envelope, identifiers, the parameters the call was made with): view counts and
titles move with every re-recording, the shape does not.
"""

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from truewire.examples import run_example_request
from truewire.spec.repo import endpoint_records, load_request_example
from typing_extensions import Any

PROJECT = Path(__file__).resolve().parents[1]

TWITCHDEV_ID = '141981764'
TWITCHDEV = 'twitchdev'


def is_frame(result: Any, *, paginated: bool) -> list[Any]:
  """Every Helix method returns the wire frame; the rows are `data`.

  No endpoint here declares `envelope.payload`, so this holds for all of them, and it is
  the one thing worth asserting on every single recording.
  """
  assert isinstance(result['data'], list)
  if paginated:
    assert 'pagination' in result, 'a paged endpoint always sends the object, empty or not'
    assert set(result['pagination']) <= {'cursor'}
  return result['data']


def users(result: Any) -> None:
  [user] = is_frame(result, paginated=False)
  assert user['login'] == TWITCHDEV
  assert user['id'] == TWITCHDEV_ID
  assert user['broadcaster_type'] in ('', 'affiliate', 'partner')
  assert user['created_at'].year == 2016, 'RFC 3339 parses into a datetime'
  assert 'email' not in user, 'an app access token never carries one'


def streams(result: Any) -> None:
  rows = is_frame(result, paginated=True)
  assert 1 <= len(rows) <= 3, 'first=3 caps the page'
  counts = [row['viewer_count'] for row in rows]
  assert counts == sorted(counts, reverse=True), 'most watched first'
  for row in rows:
    assert row['id'] and row['user_id'] and row['user_login']
    assert row['started_at'].tzinfo is not None


def videos(result: Any) -> None:
  rows = is_frame(result, paginated=True)
  assert 1 <= len(rows) <= 3
  published = [row['published_at'] for row in rows]
  assert published == sorted(published, reverse=True), 'sort=time is newest first'
  for row in rows:
    assert row['user_id'] == TWITCHDEV_ID
    assert row['type'] in ('archive', 'highlight', 'upload')
    assert row['viewable'] == 'public'
    assert row['url'].startswith('https://www.twitch.tv/videos/')


def clips(result: Any) -> None:
  rows = is_frame(result, paginated=True)
  assert 1 <= len(rows) <= 3
  for row in rows:
    assert row['broadcaster_id'] == TWITCHDEV_ID
    assert row['url'].startswith('https://clips.twitch.tv/')
    assert row['duration'] > 0
    assert row['vod_offset'] is None or row['vod_offset'] >= 0


def games(result: Any) -> None:
  [game] = is_frame(result, paginated=False)
  assert game['name'] == 'Science & Technology'
  assert '{width}x{height}' in game['box_art_url']


def top_games(result: Any) -> None:
  rows = is_frame(result, paginated=True)
  assert 1 <= len(rows) <= 3, 'first=3 caps the page'
  assert len({row['id'] for row in rows}) == len(rows)


def channels(result: Any) -> None:
  [channel] = is_frame(result, paginated=False)
  assert channel['broadcaster_id'] == TWITCHDEV_ID
  assert channel['broadcaster_login'] == TWITCHDEV
  assert isinstance(channel['delay'], int)


def subscriptions(result: Any) -> None:
  """The frame the envelope decision is really about: the totals live beside `data`."""
  rows = is_frame(result, paginated=True)
  assert isinstance(result['total'], int)
  assert result['total_cost'] <= result['max_total_cost']
  for row in rows:
    assert row['transport']['method'] in ('webhook', 'websocket', 'conduit')
    assert isinstance(row['condition'], dict)
    assert row['created_at'].tzinfo is not None


def subscribe(result: Any) -> None:
  [subscription] = is_frame(result, paginated=False)
  assert subscription['type'] == 'stream.online'
  assert subscription['condition']['broadcaster_user_id'] == TWITCHDEV_ID
  assert subscription['transport']['method'] == 'websocket'
  assert result['total'] >= 1


PROVES: dict[str, Callable[[Any], None]] = {
  'channels.get[twitchdev]': channels,
  'clips.get[twitchdev_page1]': clips,
  'eventsub.subscribe[stream_online_websocket]': subscribe,
  'eventsub.subscriptions[page1]': subscriptions,
  'games.get[science_and_technology]': games,
  'games.top[page1]': top_games,
  'streams.get[top_page1]': streams,
  'users.get[twitchdev]': users,
  'videos.get[twitchdev_page1]': videos,
}


def recorded_requests() -> list[Any]:
  """Every HTTP example request in the project, recorded or not, as a pytest param."""
  params = []
  spec = PROJECT / 'spec'
  for record in endpoint_records(PROJECT):
    if 'http' not in record.endpoint.transports:
      continue
    function = record.endpoint.resolved_function(record.path, spec)
    for request in sorted((record.path.parent / 'examples').glob('*.request.json')):
      example_id = request.name.removesuffix('.request.json')
      params.append(pytest.param(record, request, id=f'{function}[{example_id}]'))
  return params


def test_every_request_has_an_assertion():
  """A new example request needs a `PROVES` entry, or its recording proves nothing here."""
  ids = {param.id for param in recorded_requests()}
  assert ids == set(PROVES)


@pytest.mark.asyncio
@pytest.mark.parametrize('record,request_file', recorded_requests())
async def test_recording(client, record, request_file, request):
  response_file = request_file.with_name(
    request_file.name.replace('.request.json', '.response.json')
  )
  if not response_file.exists():
    pytest.skip(f'{request.node.callspec.id}: no response recorded yet; run test/recapture.sh')
  # 200 everywhere but `eventsub.subscribe`, which Twitch answers `202 Accepted`.
  assert 200 <= json.loads(response_file.read_text())['status'] < 300
  async with client:
    result = await run_example_request(
      client,
      record.endpoint,
      load_request_example(request_file),
      client_root=PROJECT,
      endpoint_path=record.path,
    )
  PROVES[request.node.callspec.id](result)
