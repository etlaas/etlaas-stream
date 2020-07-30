import logging
import sys
from typing import Dict, List, Any, Optional, TextIO, Callable

from .infrastructure import default_loads, default_dumps
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
            bookmark: Optional[Dict[str, Any]] = None,
            stream: Optional[str] = None,
            schema: Optional[Dict[str, Any]] = None,
            key_properties: Optional[List[str]] = None,
            bookmark_properties: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None,
            output_pipe: Optional[TextIO] = None,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self._name = name
        self._bookmark = bookmark or {}
        self._stream = stream
        self._schema = schema or DEFAULT_SCHEMA
        self._key_properties = key_properties or []
        self._bookmark_properties = bookmark_properties or []
        self._metadata = metadata or {}
        self._output_pipe = output_pipe or sys.stdout
        self._dumps = dumps

    def _write(self, msg: Message) -> None:
        data = self._dumps(msg.to_dict()) + '\n'
        self._output_pipe.write(data)

    def get_bookmark(self, bookmark_property: str, default_value: Optional[Any] = None) -> Any:
        assert bookmark_property in self._bookmark_properties, f'{bookmark_property} not in bookmark_properties'
        return self._bookmark.get(bookmark_property, default_value)

    def update_bookmark(self, bookmark_property: str, bookmark_value: Any) -> None:
        assert bookmark_property in self._bookmark_properties, f'{bookmark_property} not in bookmark_properties'
        self._bookmark[bookmark_property] = bookmark_value

    def update_schema(
            self,
            stream: Optional[str] = None,
            schema: Optional[Dict[str, Any]] = None,
            key_properties: Optional[List[str]] = None,
            bookmark_properties: Optional[List[str]] = None,
            metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        self._stream = stream or self._stream
        self._schema = schema or self._schema
        self._key_properties = key_properties or self._key_properties
        self._bookmark_properties = bookmark_properties or self._bookmark_properties
        self._metadata = metadata or self._metadata

    def write_schema(self) -> None:
        assert self._stream is not None, 'stream_name is undefined'
        msg = SchemaMessage(
            source=self._name,
            stream=self._stream,
            schema=self._schema,
            key_properties=self._key_properties,
            bookmark_properties=self._bookmark_properties,
            metadata=self._metadata)
        logging.info(f'writing schema {msg}')
        self._write(msg)

    def write_record(self, record: Dict[str, Any]) -> None:
        msg = RecordMessage(record=record)
        logging.debug(f'writing record {msg}')
        self._write(msg)

    def write_bookmark(self, key: str) -> None:
        msg = BookmarkMessage(key=key, value=self._bookmark)
        logging.info(f'writing bookmark {msg}')
        self._write(msg)

    def write_error(self, error: str) -> None:
        msg = ErrorMessage(error=error)
        logging.info(f'writing error {msg}')
        self._write(msg)
