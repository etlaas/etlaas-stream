import logging
import sys
from typing import Dict, List, Any, Optional, TextIO, Callable

from .infrastructure import default_dumps
from .spec import (
    SchemaMessage,
    RecordMessage,
    BookmarksMessage,
    ErrorMessage,
    Message
)

DEFAULT_SCHEMA = {
    '$schema': 'http://json-schema.org/draft/2019-09/schema#',
    'type': 'object'
}


class Source:
    def __init__(
            self,
            bookmarks: Optional[Dict[str, Any]] = None,
            output_pipe: Optional[TextIO] = None,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self.bookmarks: Dict[str, Any] = bookmarks or {}
        self._output_pipe = output_pipe or sys.stdout
        self._dumps = dumps

    def _write(self, msg: Message) -> None:
        data = self._dumps(msg.to_dict()) + '\n'
        self._output_pipe.write(data)

    def write_schema(
            self,
            source: str,
            stream: str,
            schema: Optional[Dict[str, Any]] = None,
            key_properties: Optional[List[str]] = None,
            bookmark_properties: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        msg = SchemaMessage(
            source=source,
            stream=stream,
            schema=schema or DEFAULT_SCHEMA,
            key_properties=key_properties or [],
            bookmark_properties=bookmark_properties or [],
            metadata=metadata or {})
        logging.info(f'writing schema {msg}')
        self._write(msg)

    def write_record(self, record: Dict[str, Any]) -> None:
        msg = RecordMessage(record=record)
        logging.debug(f'writing record {msg}')
        self._write(msg)

    def write_bookmarks(self, source: str, stream: str) -> None:
        msg = BookmarksMessage(
            source=source,
            stream=stream,
            bookmarks=self.bookmarks)
        logging.info(f'writing bookmark {msg}')
        self._write(msg)

    def write_error(self, error: str) -> None:
        msg = ErrorMessage(error=error)
        logging.info(f'writing error {msg}')
        self._write(msg)
