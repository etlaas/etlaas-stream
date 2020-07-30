from typing import Any, Optional


class Bookmarker:
    """
    A bookmark is the state of a source (and optionally a stream in that source) within a sink.
    Bookmarker is an interface for methods to get and set the bookmarks.
    """

    @staticmethod
    def create_key(source: str, sink: str, stream: Optional[str] = None) -> str:
        stream_key = f':{stream}' if stream else ''
        return f'{source}{stream_key}:{sink}'

    def get_bookmark(self, key: str) -> Any:
        """
        Get the b
        :key: Bookmark key.
        :return: bookmark
        """
        raise NotImplementedError()

    def set_bookmark(self, key: str, value: Any) -> None:
        """
        Set the source's stream bookmark for the target.
        :param key: Bookmark key.
        :param value: Bookmark value.
        :return: None
        """
        raise NotImplementedError()


