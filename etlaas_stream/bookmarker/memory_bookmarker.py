from typing import Any, Callable

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
        self._bookmarks = {}

    def get_bookmark(self, key: str) -> Any:
        data = self._bookmarks.get(key)
        if data:
            return self._loads(data)

    def set_bookmark(self, key: str, value: Any) -> None:
        data = self._dumps(value)
        self._bookmarks[key] = data
