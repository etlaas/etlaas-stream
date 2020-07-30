import logging
import sys
from typing import Dict, List, Any, Optional, TextIO, Callable

from .infrastructure import default_dumps
from .spec import (
    SchemaMessage,
    RecordMessage,
    BookmarkMessage,
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
            name: str,
            stream: str,
            schema: Optional[Dict[str, Any]] = None,
            key_properties: Optional[List[str]] = None,
            bookmark_properties: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None,
            output_pipe: Optional[TextIO] = None,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self.name = name
        self.stream = stream
        self.schema = schema or DEFAULT_SCHEMA
        self.key_properties = key_properties or []
        self.bookmark_properties = bookmark_properties or []
        self.metadata = metadata or {}
        self.bookmarks: Dict[str, Any] = {}
        self._output_pipe = output_pipe or sys.stdout
        self._dumps = dumps

    def _write(self, msg: Message) -> None:
        data = self._dumps(msg.to_dict()) + '\n'
        self._output_pipe.write(data)

    def update_schema(
            self,
            stream: Optional[str] = None,
            schema: Optional[Dict[str, Any]] = None,
            key_properties: Optional[List[str]] = None,
            bookmark_properties: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        self.stream = stream or self.stream
        self.schema = schema or self.schema
        self.key_properties = key_properties or self.key_properties
        self.bookmark_properties = bookmark_properties or self.bookmark_properties
        self.metadata = metadata or self.metadata

    def write_schema(self) -> None:
        msg = SchemaMessage(
            source=self.name,
            stream=self.stream,
            schema=self.schema,
            key_properties=self.key_properties,
            bookmark_properties=self.bookmark_properties,
            metadata=self.metadata)
        logging.info(f'writing schema {msg}')
        self._write(msg)

    def write_record(self, record: Dict[str, Any]) -> None:
        msg = RecordMessage(record=record)
        logging.debug(f'writing record {msg}')
        self._write(msg)

    def write_bookmark(self, key: str) -> None:
        msg = BookmarkMessage(key=key, bookmarks=self.bookmarks)
        logging.info(f'writing bookmark {msg}')
        self._write(msg)

    def write_error(self, error: str) -> None:
        msg = ErrorMessage(error=error)
        logging.info(f'writing error {msg}')
        self._write(msg)
