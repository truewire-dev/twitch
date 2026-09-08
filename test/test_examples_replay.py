"""Generic replay coverage: every recorded example, through the real client, validated.

Parametrized over the recorded pairs, so both are empty (and reported as skipped) until
the first recording lands. `test/test_recordings.py` and `test/test_events.py` say which
example is still missing its half; these two only prove that what is recorded replays.
"""

from pathlib import Path

from truewire.testing import build_http_replay_test, build_ws_replay_test

PROJECT = Path(__file__).resolve().parents[1]

test_examples_replay = build_http_replay_test(PROJECT)
test_stream_examples_replay = build_ws_replay_test(PROJECT)
