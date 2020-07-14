import redis
from typing import Callable, Any, Optional

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
        self.loads = loads
        self.dumps = dumps

    def get_bookmark(self, source: str, stream: str, sink: str) -> Any:
        key = f'{source}:{stream}:{sink}'
        if (data := self.redis.get(key)) is not None:
            return self.loads(data)

    def set_bookmark(self, source: str, stream: str, sink: str, value: Any) -> None:
        key = f'{source}:{stream}:{sink}'
        data = self.dumps(value)
        self.redis.set(key, data)
