from typing import Any


class Bookmarker:
    def get_bookmark(self, source: str, stream: str, sink: str) -> Any:
        """
        Get the source's stream bookmark for the target.
        :param source: Name of the source.
        :param stream: Name of the stream.
        :param sink: Name of the sink.
        :return: bookmark
        """
        raise NotImplementedError()

    def set_bookmark(self, source: str, stream: str, sink: str, value: Any) -> None:
        """
        Set the source's stream bookmark for the target.
        :param source: Name of the source.
        :param stream: Name of the stream.
        :param sink: Name of the sink.
        :param value: Bookmark value.
        :return: None
        """
        raise NotImplementedError()


