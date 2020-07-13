from dataclasses import dataclass, asdict
from typing import Dict, List, Any


class MessageType:
    SCHEMA = 'SCHEMA'
    RECORD = 'RECORD'
    BOOKMARK = 'BOOKMARK'


@dataclass
class Message:
    type: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SchemaMessage(Message):
    stream: str
    schema: Dict[str, Any]
    key_properties: List[str]
    bookmark_properties: List[str]
    metadata: Dict[str, Any]

    def __init__(
        self,
        stream: str,
        schema: Dict[str, Any],
        key_properties: List[str],
        bookmark_properties: List[str],
        metadata: Dict[str, Any]
    ) -> None:
        self.type = MessageType.SCHEMA
        self.stream = stream
        self.schema = schema
        self.key_properties = key_properties
        self.bookmark_properties = bookmark_properties
        self.metadata = metadata


@dataclass
class RecordMessage(Message):
    stream: str
    record: Dict[str, Any]

    def __init__(self, stream: str, record: Dict[str, Any]):
        self.type = MessageType.RECORD
        self.stream = stream
        self.record = record


@dataclass
class BookmarkMessage(Message):
    stream: str
    bookmark: Any

    def __init__(self, stream: str, bookmark: Dict[str, Any]):
        self.type = MessageType.BOOKMARK
        self.stream = stream
        self.bookmark = bookmark
