from typing import Any, Callable, Dict

from .bookmarker import Bookmarker
from ..infrastructure import default_dumps, default_loads


class MemoryBookmarker(Bookmarker):
    def __init__(
            self,
            loads: Callable[[str], Any] = default_loads,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self._loads = loads
        self._dumps = dumps
        self._data: Dict[str, Any] = {}

    def get_bookmarks(self, key: str) -> Dict[str, Any]:
        data = self._data.get(key)
        if data:
            bookmarks: Dict[str, Any] = self._loads(data)
        else:
            bookmarks = {}
        return bookmarks

    def set_bookmarks(self, key: str, bookmarks: Dict[str, Any]) -> None:
        data = self._dumps(bookmarks)
        self._data[key] = data
