"""Record real EventSub messages for every subscription example.

`truewire capture` records HTTP request/reply pairs only. Asked for the socket it stops
before opening anything:

    websocket.events is not an HTTP rpc endpoint; capture records HTTP request/reply pairs only

So the socket is recorded here instead, through the same generated client and the same
recorded parameters: open it with `examples/<id>.parameters.json`, read for a bounded
window, and write what Twitch pushed to `examples/<id>.messages.json`, the file `truewire
check` validates and `truewire mock` replays.

Opening the socket needs no credentials at all. What arrives on it does: a session nobody
has subscribed anything to is welcomed and then kept alive, and nothing else, because the
subscriptions that would fill it are created over HTTP against the session id in the
welcome — with a *user* access token the client credentials flow cannot mint. So the
recording this script makes with only the application's client id and secret is the
connect half: a `session_welcome` and the `session_keepalive` messages after it. Pass
`--keepalive 10` and a window longer than that to catch at least one keepalive.

The client is built with `validate=False` on purpose: a recording has to be the wire body,
so `message_timestamp` stays the RFC 3339 string Twitch sent rather than the `datetime` a
validated message carries. `truewire check` validates the file afterwards.

Run it from `test/recapture.sh`, or on its own with network access:

    python test/record_events.py --seconds 25 --limit 5
"""

import argparse
import asyncio
import json
from pathlib import Path

from truewire.examples import (
  client_identifier,
  coerce_ws_example_call,
  resolve_endpoint_function,
)
from truewire.spec.repo import EndpointRecord, endpoint_records, load_ws_parameters_example
from typing_extensions import Any

from twitch_helix import Twitch

PROJECT = Path(__file__).resolve().parents[1]
SPEC = PROJECT / 'spec'


async def collect(
  client: Twitch, record: EndpointRecord, parameters_file: Path, *, seconds: float, limit: int
) -> list[Any]:
  """Open one socket with an example's parameters and return the messages of `seconds`.

  Stops at `limit` messages or when the window closes, whichever comes first. Twitch never
  ends a healthy session of its own accord, so the window is what ends this one.
  """
  example = load_ws_parameters_example(parameters_file)
  subscribe = resolve_endpoint_function(
    client, record.endpoint, endpoint_path=record.path, spec_root=SPEC
  )
  args, kwargs = coerce_ws_example_call(subscribe, example, identifier=client_identifier(PROJECT))
  messages: list[Any] = []
  stream = await subscribe(*args, **kwargs)
  try:

    async def read() -> None:
      async for message in stream:
        messages.append(message)
        if len(messages) >= limit:
          return

    try:
      await asyncio.wait_for(read(), timeout=seconds)
    except (asyncio.TimeoutError, TimeoutError):
      pass
  finally:
    await stream.unsubscribe()
  return messages


async def record_all(*, seconds: float, limit: int, ws_url: str | None) -> int:
  """Record every EventSub example; return the process exit code."""
  streams = [
    record for record in endpoint_records(PROJECT) if record.endpoint.spec.kind == 'stream'
  ]
  recorded = 0
  for record in streams:
    function = record.endpoint.resolved_function(record.path, SPEC)
    for parameters_file in sorted((record.path.parent / 'examples').glob('*.parameters.json')):
      example_id = parameters_file.name.removesuffix('.parameters.json')
      client = (
        Twitch.new(validate=False) if ws_url is None else Twitch.new(ws_url=ws_url, validate=False)
      )
      async with client:
        messages = await collect(client, record, parameters_file, seconds=seconds, limit=limit)
      if not messages:
        print(f'{function}[{example_id}]: no messages in {seconds}s; nothing written')
        continue
      out = parameters_file.with_name(f'{example_id}.messages.json')
      out.write_text(json.dumps(messages, indent=2, ensure_ascii=False) + '\n')
      print(f'{function}[{example_id}]: {len(messages)} message(s) -> {out.relative_to(PROJECT)}')
      recorded += 1
  if not recorded:
    print('no EventSub messages recorded')
    return 1
  return 0


def main() -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--seconds', type=float, default=25.0, help='How long to read each socket.')
  parser.add_argument('--limit', type=int, default=5, help='How many messages to keep per socket.')
  parser.add_argument(
    '--ws-url', default=None, help='EventSub socket to open; omit for the default.'
  )
  args = parser.parse_args()
  return asyncio.run(record_all(seconds=args.seconds, limit=args.limit, ws_url=args.ws_url))


if __name__ == '__main__':
  raise SystemExit(main())
