#!/usr/bin/env sh
# Re-record every example in this project from the live Twitch APIs with `truewire
# capture` and, for the EventSub socket, `test/record_events.py`. Run from anywhere; needs
# `truewire` and the package on the path (`pip install -e '.[dev]'`).
#
# Credentials: an application's client id and secret, from
# https://dev.twitch.tv/console/apps, in TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET. The
# core reads those two variables and mints an app access token through the client
# credentials flow; no value is ever written into the tree, and the token travels in a
# header, which no recorded example holds. The script refuses to run without them rather
# than recording nothing and exiting green.
#
# The request half of each example (`examples/<id>.request.json`) and the parameters of
# each socket (`examples/<id>.parameters.json`) are the source of truth: this script
# replays exactly those, so re-recording keeps the same ids and descriptions and only the
# responses move. `truewire capture` drops an endpoint's `unverified` declaration itself
# once it has written a pair.
#
# `eventsub.subscribe` needs a *user* access token, which the client credentials flow
# cannot mint, so it is skipped here and keeps its `unverified` declaration. Set
# TWITCH_USER_ACCESS_TOKEN and RECORD_USER_ENDPOINTS=1 to record it too, with a token
# whose session id in the example's `transport.session_id` is a socket you have open.
set -e
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
EVENTSUB_SECONDS=${EVENTSUB_SECONDS:-25}
EVENTSUB_LIMIT=${EVENTSUB_LIMIT:-5}

if [ -z "$TWITCH_CLIENT_ID" ] || [ -z "$TWITCH_CLIENT_SECRET" ]; then
  echo "TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must both be set: every Helix endpoint" >&2
  echo "needs an app access token, and this script mints one from them. Register an" >&2
  echo "application at https://dev.twitch.tv/console/apps to get a pair." >&2
  exit 1
fi

for request in spec/endpoints/*/*/examples/*.request.json; do
  function=$(echo "$request" | awk -F/ '{print $3 "." $4}')
  id=$(basename "$request" .request.json)
  case "$function" in
    eventsub.subscribe)
      if [ -z "$RECORD_USER_ENDPOINTS" ]; then
        echo "$function[$id]: skipped; needs a user access token (see the header of this script)"
        continue
      fi
      ;;
  esac
  description=$("$PYTHON" -c 'import json, sys; print(json.load(open(sys.argv[1])).get("description", ""))' "$request")
  parameters=$("$PYTHON" -c 'import json, sys; print(json.dumps(json.load(open(sys.argv[1]))["request"]))' "$request")
  truewire capture "$function" --id "$id" -d "$description" --request "$parameters"
done

# `truewire capture` refuses a stream endpoint, so the EventSub socket is read directly
# through the generated client for a bounded window and the messages it pushed are written
# beside the subscription's parameters.
"$PYTHON" test/record_events.py --seconds "$EVENTSUB_SECONDS" --limit "$EVENTSUB_LIMIT"

"$PYTHON" test/verified.py
