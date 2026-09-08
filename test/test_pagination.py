"""The cursor walk, over three pages, without the network.

A single page cannot tell a correct walk from one that happened to stop immediately
(production standard S18), and the recordings are one page each by design. So the walk is
driven here against an in-memory `httpx` transport that answers three pages the way Helix
does: two with a cursor, and a last one whose `pagination` is an *empty object* rather
than a missing key or a null cursor. That last frame is the whole reason these endpoints
declare `done: {"kind": "absent_cursor"}` on `pagination.cursor`, and the reason none of
them unwraps `data`: the cursor lives beside the rows, not inside them.
"""

import httpx
import pytest

from twitch_helix import Twitch

PAGES = [
  {
    'data': [{'id': '1', 'name': 'One', 'box_art_url': 'a-{width}x{height}.jpg'}],
    'pagination': {'cursor': 'CURSOR_TO_PAGE_2'},
  },
  {
    'data': [{'id': '2', 'name': 'Two', 'box_art_url': 'b-{width}x{height}.jpg'}],
    'pagination': {'cursor': 'CURSOR_TO_PAGE_3'},
  },
  {
    'data': [{'id': '3', 'name': 'Three', 'box_art_url': 'c-{width}x{height}.jpg'}],
    'pagination': {},
  },
]


@pytest.fixture
def paging_client() -> tuple[Twitch, list[str | None]]:
  """A client whose `/helix/games/top` answers `PAGES`, and the cursors it was asked for."""
  asked: list[str | None] = []
  client = Twitch.new(client_id='ID', app_token='TOKEN')

  def handler(request: httpx.Request) -> httpx.Response:
    cursor = request.url.params.get('after')
    asked.append(cursor)
    index = (
      0
      if cursor is None
      else next(i + 1 for i, page in enumerate(PAGES) if page['pagination'].get('cursor') == cursor)
    )
    return httpx.Response(200, json=PAGES[index])

  client.client.http._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
  return client, asked


@pytest.mark.asyncio
async def test_awaiting_a_walk_flattens_every_page(paging_client):
  client, asked = paging_client
  async with client:
    rows = await client.games.top_paged(first=1)
  assert [row['id'] for row in rows] == ['1', '2', '3']
  assert asked == [None, 'CURSOR_TO_PAGE_2', 'CURSOR_TO_PAGE_3'], (
    'each page is fetched with the cursor the one before it handed back'
  )


@pytest.mark.asyncio
async def test_an_empty_pagination_object_ends_the_walk(paging_client):
  """Helix never omits `pagination`; it sends `{}`. Nothing after the third page is asked for."""
  client, asked = paging_client
  async with client:
    pages = [page async for page in client.games.top_paged(first=1)]
  assert [[row['id'] for row in page] for page in pages] == [['1'], ['2'], ['3']]
  assert len(asked) == 3, 'the walk stopped rather than asking for a fourth page'


@pytest.mark.asyncio
async def test_a_walk_can_be_checkpointed_and_resumed(paging_client):
  """`pages()` hands back the cursor on either side of each page, so a walk survives a restart."""
  client, _ = paging_client
  async with client:
    walk = client.games.top_paged(first=1)
    checkpoints = [
      (page.state, [row['id'] for row in page.rows], page.next) async for page in walk.pages()
    ]
    assert checkpoints == [
      ('', ['1'], 'CURSOR_TO_PAGE_2'),
      ('CURSOR_TO_PAGE_2', ['2'], 'CURSOR_TO_PAGE_3'),
      ('CURSOR_TO_PAGE_3', ['3'], None),
    ]
    resumed = await walk.resume('CURSOR_TO_PAGE_2')
  assert [row['id'] for row in resumed] == ['2', '3']


@pytest.mark.asyncio
async def test_the_frame_carries_the_cursor_for_a_hand_rolled_walk(paging_client):
  """The plain method returns the wire frame, so a caller can page it themselves."""
  client, _ = paging_client
  async with client:
    frame = await client.games.top(first=1)
    assert frame['data'][0]['id'] == '1'
    assert frame['pagination']['cursor'] == 'CURSOR_TO_PAGE_2'
    last = await client.games.top(first=1, after='CURSOR_TO_PAGE_3')
    assert last['pagination'] == {}, 'the empty object that means "no more pages"'
