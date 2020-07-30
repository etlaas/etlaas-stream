import fastjsonschema
import logging
import sys
from typing import Iterator, Dict, Any, Optional, TextIO, Callable

from .infrastructure import default_loads
from .spec import (
    MessageType,
    SchemaMessage,
    RecordMessage,
    BookmarkMessage,
    ErrorMessage,
    Message
)


class Sink:
    def __init__(
            self,
            name: str,
            input_pipe: Optional[TextIO] = None,
            loads: Callable[[str], Any] = default_loads
    ) -> None:
        self.name = name
        self.source: Optional[str] = None
        self.stream: Optional[str] = None
        self.schema: Dict[str, Any] = {}
        self._input_pipe = input_pipe or sys.stdin
        self._loads = loads
        self._validate: Optional[Callable[[Any], None]] = None

    def deserialize_message(self, line: str) -> Message:
        try:
            data: Dict[str, Any] = self._loads(line)
            msg_type = data.pop('type')
            if msg_type == MessageType.SCHEMA:
                return SchemaMessage(
                    source=data['source'],
                    stream=data['stream'],
                    schema=data['schema'],
                    key_properties=data['key_properties'],
                    bookmark_properties=data['bookmark_properties'],
                    metadata=data['metadata'])
            elif msg_type == MessageType.RECORD:
                return RecordMessage(record=data['record'])
            elif msg_type == MessageType.BOOKMARK:
                return BookmarkMessage(key=data['key'], bookmarks=data['bookmarks'])
            elif msg_type == MessageType.ERROR:
                return ErrorMessage(error=data['error'])
            else:
                raise ValueError(f'Cannot read message: {data}')
        except Exception as exn:
            logging.error(f'failed to deserialize message: {line}\n{exn}')
            raise exn

    def read(self) -> Iterator[Message]:
        while True:
            line = self._input_pipe.readline()
            if line == "":
                return
            else:
                msg = self.deserialize_message(line)
                if isinstance(msg, SchemaMessage):
                    self.source = msg.source
                    self.stream = msg.stream
                    self.schema = msg.schema
                    self._validate = fastjsonschema.compile(msg.schema)
                elif isinstance(msg, RecordMessage):
                    assert self.source is not None, 'source is not initialized'
                    assert self.stream is not None, 'stream is not initialized'
                    assert self._validate is not None, 'validate is not initialized'
                    try:
                        self._validate(msg.record)
                    except Exception as exn:
                        logging.error(f'validation failed for {msg.record}')
                        raise exn
                elif isinstance(msg, BookmarkMessage):
                    assert self.source is not None, 'source is not initialized'
                    assert self.stream is not None, 'stream is not initialized'
                elif isinstance(msg, ErrorMessage):
                    logging.error(msg.error)
                    raise RuntimeError(msg.error)
                logging.debug(f'received message: {msg}')
                yield msg
