import redis
from dataclasses import dataclass
from typing import Callable, Any

from .bookmarker import Bookmarker
from ..infrastructure import default_dumps, default_loads


@dataclass
class RedisConfig:
    host: str
    port: int
    password: str
    database: int


class RedisBookmarker(Bookmarker):
    def __init__(
        self,
        config: RedisConfig,
        loads: Callable[[str], Any] = default_loads,
        dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self.redis = redis.Redis(
            host=config.host,
            port=config.port,
            db=config.database,
            password=config.password)
        self.loads = loads
        self.dumps = dumps

    def get_bookmark(self, source: str, stream: str, sink: str) -> Any:
        key = f'{source}:{stream}:{sink}'
        data = self.redis.get(key)
        return self.loads(data)

    def set_bookmark(self, source: str, stream: str, sink: str, value: Any) -> None:
        key = f'{source}:{stream}:{sink}'
        data = self.dumps(value)
        self.redis.set(key, data)
