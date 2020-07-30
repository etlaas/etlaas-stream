from dataclasses import dataclass, asdict
from typing import Dict, List, Any


class MessageType:
    SCHEMA = 'SCHEMA'
    RECORD = 'RECORD'
    BOOKMARK = 'BOOKMARK'
    ERROR = 'ERROR'


@dataclass
class Message:
    type: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SchemaMessage(Message):
    source: str
    stream: str
    schema: Dict[str, Any]
    key_properties: List[str]
    bookmark_properties: List[str]
    metadata: Dict[str, Any]

    def __init__(
        self,
        source: str,
        stream: str,
        schema: Dict[str, Any],
        key_properties: List[str],
        bookmark_properties: List[str],
        metadata: Dict[str, Any]
    ) -> None:
        self.type = MessageType.SCHEMA
        self.source = source
        self.stream = stream
        self.schema = schema
        self.key_properties = key_properties
        self.bookmark_properties = bookmark_properties
        self.metadata = metadata


@dataclass
class RecordMessage(Message):
    record: Dict[str, Any]

    def __init__(self, record: Dict[str, Any]):
        self.type = MessageType.RECORD
        self.record = record


@dataclass
class BookmarkMessage(Message):
    key: str
    bookmarks: Dict[str, Any]

    def __init__(self, key: str, bookmarks: Dict[str, Any]):
        self.type = MessageType.BOOKMARK
        self.key = key
        self.bookmarks = bookmarks


@dataclass
class ErrorMessage(Message):
    error: str

    def __init__(self, error: str):
        self.type = MessageType.ERROR
        self.error = error
