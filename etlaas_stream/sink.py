import fastjsonschema
import logging
import sys
from typing import Iterator, Dict, List, Any, Optional, TextIO, Callable

from .infrastructure import default_loads, default_dumps
from .spec import (
    MessageType,
    SchemaMessage,
    RecordMessage,
    BookmarkMessage,
    Message
)


class Sink:
    def __init__(
            self,
            input_pipe: Optional[TextIO] = None,
            loads: Callable[[str], Any] = default_loads,
            dumps: Callable[[Any], str] = default_dumps
    ) -> None:
        self.input_pipe = input_pipe or sys.stdin
        self.loads = loads
        self.dumps = dumps

        self.stream_name: Optional[str] = None
        self.schema: Dict[str, Any] = {}
        self.validate: Optional[Callable[[Any], None]] = None
        self.key_properties: List[str] = []
        self.bookmark_properties: List[str] = []
        self.metadata: Dict[str, Any] = {}
        self.bookmark: Dict[str, Any] = {}
        self.record_count = 0

    def get_bookmark(self, bookmark_property: str) -> Any:
        assert self.stream_name is not None, 'stream_name is undefined'
        assert bookmark_property in self.bookmark_properties, f'{bookmark_property} not in bookmarks_properties'
        return self.bookmark.get(bookmark_property)

    def deserialize_message(self, line: str) -> Message:
        try:
            data: Dict[str, Any] = self.loads(line)
            msg_type = data.pop('type')
            if msg_type == MessageType.SCHEMA:
                return SchemaMessage(
                    stream=data['stream'],
                    schema=data['schema'],
                    key_properties=data['key_properties'],
                    bookmark_properties=data['bookmark_properties'],
                    metadata=data['metadata'])
            elif msg_type == MessageType.RECORD:
                return RecordMessage(
                    stream=data['stream'],
                    record=data['record'])
            elif msg_type == MessageType.BOOKMARK:
                return BookmarkMessage(
                    stream=data['stream'],
                    bookmark=data['bookmark'])
            else:
                raise ValueError(f'Cannot read message: {data}')
        except Exception as exn:
            logging.error(f'failed to deserialize message: {line}\n{exn}')
            raise exn

    def read(self) -> Iterator[Message]:
        while True:
            line = self.input_pipe.readline()
            if line == "":
                return
            else:
                msg = self.deserialize_message(line)
                if isinstance(msg, SchemaMessage):
                    self.stream_name = msg.stream
                    self.schema = msg.schema
                    self.validate = fastjsonschema.compile(msg.schema)
                    self.key_properties = msg.key_properties
                    self.bookmark_properties = msg.bookmark_properties
                    self.metadata = msg.metadata
                elif isinstance(msg, RecordMessage):
                    assert self.stream_name is not None, 'stream_name is not initialized'
                    assert self.validate is not None, 'validate is not initialized'
                    assert self.stream_name == msg.stream,\
                        f'record stream does not match schema: {msg.stream} != {self.stream_name}'
                    try:
                        self.validate(msg.record)
                    except Exception as exn:
                        logging.error(f'validation failed for {msg.record}')
                        raise exn
                    self.record_count += 1
                elif isinstance(msg, BookmarkMessage):
                    assert self.stream_name is not None, 'stream_name is not initialized'
                    self.bookmark = msg.bookmark
                logging.debug(f'received message: {msg}')
                yield msg
