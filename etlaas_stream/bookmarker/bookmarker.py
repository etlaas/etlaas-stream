from typing import Any, Dict


class Bookmarker:
    """
    A bookmark is the state of a source's stream for a particular sink.
    Bookmarker is an interface with methods to get and set the bookmarks.
    """

    @staticmethod
    def get_key(source: str, stream: str, sink: str) -> str:
        """
        Get the bookmarks key.
        :param source: Name of the source.
        :param stream: Name of the stream.
        :param sink: Name of the sink.
        """
        return f'{source}:{stream}:{sink}'

    def get_bookmarks(self, key: str) -> Dict[str, Any]:
        """
        Get bookmarks.
        :param key: Bookmarks key.
        :return: bookmarks
        """
        raise NotImplementedError()

    def set_bookmarks(self, key: str, bookmarks: Dict[str, Any]) -> None:
        """
        Set bookmarks.
        :param key: Bookmarks key.
        :param bookmarks: Bookmarks to set.
        :return: None
        """
        raise NotImplementedError()


