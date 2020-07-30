import redis
from typing import Dict, Callable, Any, Optional

from .bookmarker import Bookmarker
from ..infrastructure import default_dumps, default_loads


class RedisBookmarker(Bookmarker):
    def __init__(
            self,
            host: str = 'localhost',
            port: int = 6379,
            password: Optional[str] = None,
            database: int = 0,
            loads: Callable[[str], Any] = default_loads,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=database,
            password=password)
        self._loads = loads
        self._dumps = dumps

    def get_bookmarks(self, key: str) -> Dict[str, Any]:
        if (data := self.redis.get(key)) is not None:
            bookmarks: Dict[str, Any] = self._loads(data)
        else:
            bookmarks = {}
        return bookmarks

    def set_bookmarks(self, key: str, bookmarks: Dict[str, Any]) -> None:
        data = self._dumps(bookmarks)
        self.redis.set(key, data)
