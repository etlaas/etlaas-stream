import csv
import io
import pytest
from datetime import datetime
from etlaas_stream import Bookmarker, MemoryBookmarker, Source, Sink, SchemaMessage, RecordMessage, BookmarkMessage
from pathlib import Path
from typing import Optional, TextIO, List, Dict, Any


TEST_DATA_DIR = Path(__file__).parent / 'test_data'


class LineSource(Source):
    LAST_MODIFIED_MAP = {
        'dogs1.csv': datetime(2020, 1, 1),
        'dogs2.csv': datetime(2020, 1, 2)
    }

    def __init__(
            self,
            name: str,
            sink: str,
            bookmarker: Bookmarker,
            file_dir: Path,
            **kwargs: Any
    ) -> None:
        super().__init__(name=name, bookmark_properties=['_last_modified_at'], **kwargs)
        self.name = name
        self.sink = sink
        self.bookmarker = bookmarker
        self.file_dir = file_dir
        self.file_handle: Optional[TextIO] = None

    def start(self) -> None:
        bookmark_key = self.bookmarker.create_key(self.name, self.sink)
        initial_bookmark: Optional[datetime] = self.bookmarker.get_bookmark(bookmark_key)
        bookmark: Optional[datetime] = initial_bookmark
        for file in sorted(self.file_dir.glob('*.csv')):
            last_modified: datetime = self.LAST_MODIFIED_MAP[file.name]
            if (initial_bookmark is None) or (initial_bookmark is not None and last_modified > initial_bookmark):
                metadata = {'source': f'{self.file_dir.name}/{file.name}'}
                self.update_schema(
                    stream=file.name,
                    metadata=metadata)
                self.write_schema()
                if self.file_handle is not None:
                    if not self.file_handle.closed:
                        self.file_handle.close()
                self.file_handle = file.open('r')
                for line in self.file_handle:
                    record = {'_last_modified_at': last_modified.isoformat(), 'line': line.strip()}
                    self.write_record(record)
                if (bookmark is None) or (last_modified > bookmark):
                    bookmark = last_modified
                if not self.file_handle.closed:
                    self.file_handle.close()
        if bookmark:
            self.update_bookmark('_last_modified_at', bookmark.isoformat())
            self.write_bookmark(bookmark_key)


class CsvSource(Source):
    def __init__(
            self,
            name: str,
            stream: str,
            sink: str,
            bookmark_properties: List[str],
            bookmarker: Bookmarker,
            file_dir: Path,
            **kwargs: Any
    ) -> None:
        super().__init__(name=name, stream=stream, bookmark_properties=bookmark_properties, **kwargs)
        self.name = name
        self.stream = stream
        self.sink = sink
        self.bookmark_properties = bookmark_properties
        self.bookmarker = bookmarker
        self.file_dir = file_dir
        self.file_handle: Optional[TextIO] = None

    def start(self) -> None:
        bookmark_key = self.bookmarker.create_key(self.name, self.sink, self.stream)
        bookmark: Dict[str, Any] = self.bookmarker.get_bookmark(bookmark_key) or {'birth_date': ''}
        for bookmark_property in self.bookmark_properties:
            self.update_bookmark(bookmark_property, bookmark.get(bookmark_property))
        write_schema = True
        for file in sorted(self.file_dir.glob('*.csv')):
            source = f'{self.file_dir.name}/{file.name}'
            if self.file_handle is not None:
                if not self.file_handle.closed:
                    self.file_handle.close()
            self.file_handle = file.open('r')
            reader = csv.DictReader(self.file_handle)
            for row in reader:
                fields = ['_source', *row.keys()]
                if write_schema:
                    schema = {
                        '$schema': 'http://json-schema.org/draft/2019-09/schema#',
                        'type': 'object',
                        'properties': {k: {'type': 'string'} for k in fields},
                        'required': fields
                    }
                    self.update_schema(schema=schema)
                    self.write_schema()
                    write_schema = False
                should_write = any([row[p] > self.get_bookmark(p, '') for p in self.bookmark_properties])
                if should_write:
                    self.write_record({'_source': source, **row})
                    for bookmark_property in self.bookmark_properties:
                        self.update_bookmark(bookmark_property, row[bookmark_property])
            self.write_bookmark(bookmark_key)
            if not self.file_handle.closed:
                self.file_handle.close()


class LineSink(Sink):
    def __init__(
            self,
            bookmarker: Bookmarker,
            temp_dir: str,
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.bookmarker = bookmarker
        self.temp_dir = temp_dir
        self.file_handle:  Optional[TextIO] = None

    def start(self) -> None:
        for msg in self.read():
            if isinstance(msg, SchemaMessage):
                if (self.file_handle is not None) and (not self.file_handle.closed):
                    self.file_handle.close()
                self.file_handle = Path(self.temp_dir, msg.stream).open('w')
            elif isinstance(msg, RecordMessage):
                assert 'line' in msg.record, f'line property not found in {msg}'
                self.file_handle.write(msg.record['line'] + '\n')
            elif isinstance(msg, BookmarkMessage):
                self.bookmarker.set_bookmark(msg.key, msg.value)
        if not self.file_handle.closed:
            self.file_handle.close()


class CsvSink(Sink):
    def __init__(self, bookmarker: Bookmarker, temp_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.bookmarker = bookmarker
        self.temp_dir = temp_dir
        self.file_handle: Optional[TextIO] = None
        self.writer: Optional[csv.DictWriter] = None

    def start(self) -> None:
        for msg in self.read():
            if isinstance(msg, SchemaMessage):
                assert self.writer is None, 'writer already initialized'
                self.file_handle = Path(self.temp_dir, msg.stream).open('w', newline='')
                properties = msg.schema.get('properties', {}).keys()
                assert properties, 'schema properties must be defined'
                self.writer = csv.DictWriter(self.file_handle, fieldnames=properties)
                self.writer.writeheader()
            elif isinstance(msg, RecordMessage):
                assert self.writer is not None, 'writer is not initialized'
                self.writer.writerow(msg.record)
            elif isinstance(msg, BookmarkMessage):
                self.bookmarker.set_bookmark(msg.key, msg.value)
        if not self.file_handle.closed:
            self.file_handle.close()


def test_line_stream(tmpdir):
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    source = LineSource(
        name='input',
        sink='line',
        bookmarker=bookmarker,
        file_dir=TEST_DATA_DIR / 'input',
        output_pipe=pipe)

    sink = LineSink(
        name='output',
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    source.start()
    pipe.seek(0)
    sink.start()

    actual_messages = pipe.getvalue()
    expected_messages_path = Path(TEST_DATA_DIR, 'output', 'line_messages.txt')
    expected_messages = expected_messages_path.read_text()

    assert actual_messages == expected_messages

    for file in Path(tmpdir).glob('*.csv'):
        expected_rows = Path(TEST_DATA_DIR, 'input', file.name).read_text()
        actual_rows = file.read_text()
        assert actual_rows == expected_rows

    actual_bookmark = bookmarker.get_bookmark('input:line')
    assert actual_bookmark == {'_last_modified_at': '2020-01-02T00:00:00'}


def test_csv_stream(tmpdir):
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    source = CsvSource(
        name='input',
        stream='dogs.csv',
        sink='csv',
        file_dir=TEST_DATA_DIR / 'input',
        key_properties=['id'],
        bookmark_properties=['birth_date'],
        bookmarker=bookmarker,
        output_pipe=pipe)

    sink = CsvSink(
        name='output',
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    source.start()
    pipe.seek(0)
    sink.start()

    actual_messages = pipe.getvalue()
    expected_messages_path = Path(TEST_DATA_DIR, 'output', 'csv_messages.txt')
    expected_messages = expected_messages_path.read_text()

    assert actual_messages == expected_messages

    expected_rows = Path(TEST_DATA_DIR, 'output', 'dogs.csv').read_text()
    actual_rows = Path(tmpdir, 'dogs.csv').read_text()

    assert actual_rows == expected_rows

    actual_bookmark = bookmarker.get_bookmark('input:dogs.csv:csv')
    assert actual_bookmark == {'birth_date': '2020-01-04'}


def test_csv_stream_bookmark(tmpdir):
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    bookmarker.set_bookmark('input:dogs.csv:csv', {'birth_date': '2020-01-02'})

    source = CsvSource(
        name='input',
        stream='dogs.csv',
        sink='csv',
        file_dir=TEST_DATA_DIR / 'input',
        key_properties=['id'],
        bookmark_properties=['birth_date'],
        bookmarker=bookmarker,
        output_pipe=pipe)

    sink = CsvSink(
        name='output',
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    source.start()
    pipe.seek(0)
    sink.start()

    assert source.get_bookmark('birth_date') == '2020-01-04'

    actual_messages = pipe.getvalue()
    expected_messages_path = Path(TEST_DATA_DIR, 'output', 'csv_messages_bookmark.txt')
    expected_messages = expected_messages_path.read_text()

    assert actual_messages == expected_messages

    expected_rows = Path(TEST_DATA_DIR, 'output', 'dogs_bookmark.csv').read_text()
    actual_rows = Path(tmpdir, 'dogs.csv').read_text()

    assert actual_rows == expected_rows

    actual_bookmark = bookmarker.get_bookmark('input:dogs.csv:csv')
    assert actual_bookmark == {'birth_date': '2020-01-04'}


def test_csv_stream_error(tmpdir):
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    with Path(TEST_DATA_DIR, 'input', 'error_messages.txt').open('r') as fr:
        data = fr.read()
        pipe.write(data)
    pipe.seek(0)

    sink = LineSink(
        name='output',
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    with pytest.raises(RuntimeError):
        sink.start()
