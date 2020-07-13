import json
from etlaas_stream import Bookmarker
from typing import Any


class MemoryBookmarker(Bookmarker):
    def __init__(self):
        self.bookmarks = {}

    def get_bookmark(self, source: str, stream: str, sink: str) -> Any:
        key = f'{source}:{stream}:{sink}'
        data = self.bookmarks.get(key)
        return json.loads(data)

    def set_bookmark(self, source: str, stream: str, sink: str, value: Any) -> None:
        key = f'{source}:{stream}:{sink}'
        data = json.dumps(value)
        self.bookmarks[key] = data


def test_bookmarker():
    source = 'input'
    stream = 'test'
    sink = 'output'

    bookmarker = MemoryBookmarker()
    bookmark = {'a': 1, 'b': 2}
    bookmarker.set_bookmark(
        source=source,
        stream=stream,
        sink=sink,
        value=bookmark)

    retrieved_bookmark = bookmarker.get_bookmark(
        source=source,
        stream=stream,
        sink=sink)

    assert retrieved_bookmark == bookmark
