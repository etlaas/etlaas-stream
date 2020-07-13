import logging
import sys
from typing import Dict, List, Any, Optional, TextIO, Callable

from .infrastructure import default_loads, default_dumps
from .spec import (
    SchemaMessage,
    RecordMessage,
    BookmarkMessage,
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
            loads: Callable[[str], Any] = default_loads,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self.name = name
        self.bookmark = bookmark or {}
        self.stream = stream
        self.schema = schema or DEFAULT_SCHEMA
        self.key_properties = key_properties or []
        self.bookmark_properties = bookmark_properties or []
        self.metadata = metadata or {}
        self.output_pipe = output_pipe or sys.stdout
        self.loads = loads
        self.dumps = dumps
        self.record_count: int = 0

    def _write(self, msg: Message) -> None:
        data = self.dumps(msg.to_dict()) + '\n'
        self.output_pipe.write(data)

    def get_bookmark(self, bookmark_property: str, default_value: Optional[Any] = None) -> Any:
        assert bookmark_property in self.bookmark_properties, f'{bookmark_property} not in bookmark_properties'
        return self.bookmark.get(bookmark_property, default_value)

    def update_bookmark(self, bookmark_property: str, bookmark_value: Any) -> None:
        assert bookmark_property in self.bookmark_properties, f'{bookmark_property} not in bookmark_properties'
        self.bookmark[bookmark_property] = bookmark_value

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
        assert self.stream is not None, 'stream_name is undefined'
        msg = SchemaMessage(
            source=self.name,
            stream=self.stream,
            schema=self.schema,
            key_properties=self.key_properties,
            bookmark_properties=self.bookmark_properties,
            metadata=self.metadata)
        logging.info(f'writing schema {msg}')
        self._write(msg)
        self.record_count = 0

    def write_record(self, record: Dict[str, Any]) -> None:
        msg = RecordMessage(record=record)
        logging.debug(f'writing record {msg}')
        self._write(msg)
        self.record_count += 1

    def write_bookmark(self) -> None:
        msg = BookmarkMessage(bookmark=self.bookmark)
        logging.info(f'writing bookmark {msg}')
        self._write(msg)
