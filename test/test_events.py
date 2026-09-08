"""What the EventSub recording proves, replayed through the mock server.

One test per subscription example. EventSub pushes from the moment the socket opens, so
the mock replays the recorded messages on connect and the client reads exactly as many as
were recorded; asking for one more would wait for a socket that has nothing left to say.

A subscription whose messages have not been recorded yet skips with the reason, the same
way `test/test_recordings.py` skips an unrecorded HTTP example.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import pytest
from truewire.examples import (
  client_identifier,
  coerce_ws_example_call,
  resolve_endpoint_function,
)
from truewire.spec.repo import endpoint_records, load_ws_parameters_example
from typing_extensions import Any

PROJECT = Path(__file__).resolve().parents[1]
SPEC = PROJECT / 'spec'

KINDS = {
  'session_welcome',
  'session_keepalive',
  'notification',
  'session_reconnect',
  'revocation',
}


def a_session(messages: list[Any]) -> None:
  """A socket nobody has subscribed anything to: a welcome, then keepalives.

  The welcome is what makes the rest of EventSub work — its session id is what an
  `eventsub.subscribe(...)` call names as its `transport.session_id` — so the one thing
  worth asserting is that it arrives first and carries one.
  """
  first, *rest = messages
  metadata = first['metadata']
  assert metadata['message_type'] == 'session_welcome', 'the welcome always comes first'
  assert isinstance(metadata['message_timestamp'], datetime), 'RFC 3339 parses'
  assert metadata['message_timestamp'].tzinfo is not None
  session = first['payload']['session']
  assert session['id'], 'the id an EventSub subscription is created against'
  assert session['status'] == 'connected'
  assert session['reconnect_url'] is None
  for message in rest:
    kind = message['metadata']['message_type']
    assert kind in KINDS
    if kind == 'session_keepalive':
      assert message.get('payload', {}) == {}, 'a keepalive carries nothing'
    if kind == 'notification':
      payload = message['payload']
      assert payload['subscription']['type'] == message['metadata']['subscription_type']
      assert isinstance(payload['event'], dict)


PROVES = {'websocket.events[default]': a_session}


def subscription_examples() -> list[Any]:
  """Every recorded subscription's parameters, recorded messages or not, as a param."""
  params = []
  for record in endpoint_records(PROJECT):
    if record.endpoint.spec.kind != 'stream':
      continue
    function = record.endpoint.resolved_function(record.path, SPEC)
    for parameters in sorted((record.path.parent / 'examples').glob('*.parameters.json')):
      example_id = parameters.name.removesuffix('.parameters.json')
      params.append(pytest.param(record, parameters, id=f'{function}[{example_id}]'))
  return params


def test_every_subscription_has_an_assertion():
  """A new subscription example needs a `PROVES` entry, or its recording proves nothing."""
  ids = {param.id for param in subscription_examples()}
  assert ids == set(PROVES)


@pytest.mark.asyncio
@pytest.mark.parametrize('record,parameters_file', subscription_examples())
async def test_subscription(client, record, parameters_file, request):
  messages_file = parameters_file.with_name(
    parameters_file.name.replace('.parameters.json', '.messages.json')
  )
  if not messages_file.exists():
    pytest.skip(
      f'{request.node.callspec.id}: no messages recorded yet '
      '(the EventSub socket was never read); run test/recapture.sh'
    )
  expected = json.loads(messages_file.read_text())
  assert expected, 'a recorded subscription carries at least one message'

  async with client:
    subscribe = resolve_endpoint_function(
      client, record.endpoint, endpoint_path=record.path, spec_root=SPEC
    )
    args, kwargs = coerce_ws_example_call(
      subscribe, load_ws_parameters_example(parameters_file), identifier=client_identifier(PROJECT)
    )
    stream = await subscribe(*args, **kwargs)
    try:
      messages = []
      pushed = stream.__aiter__()
      for _ in expected:
        messages.append(await asyncio.wait_for(anext(pushed), timeout=10))
    finally:
      await stream.unsubscribe()

  assert len(messages) == len(expected)
  PROVES[request.node.callspec.id](messages)
