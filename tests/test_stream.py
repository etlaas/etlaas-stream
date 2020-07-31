import csv
import io
import pytest
from datetime import datetime
from etlaas_stream import Bookmarker, MemoryBookmarker, Source, Sink, SchemaMessage, RecordMessage, BookmarksMessage
from pathlib import Path
from typing import List, Optional, TextIO, Any


TEST_DATA_DIR = Path(__file__).parent / 'test_data'


class LineSource(Source):
    LAST_MODIFIED_MAP = {
        'dogs1.csv': datetime(2020, 1, 1),
        'dogs2.csv': datetime(2020, 1, 2)
    }

    def __init__(
            self,
            source: str,
            stream: str,
            file_dir: Path,
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.stream = stream
        self.bookmark_properties = ['_last_modified_at']
        self.file_dir = file_dir
        self.file_name: Optional[str] = None
        self.file_handle: Optional[TextIO] = None

    def start(self) -> None:
        initial_bookmark = self.bookmarks.get(self.bookmark_properties[0])
        bookmark: Optional[datetime] = initial_bookmark
        for file in sorted(self.file_dir.glob('*.csv')):
            self.file_name = file.name
            last_modified: datetime = self.LAST_MODIFIED_MAP[self.file_name]
            if (not initial_bookmark) or (initial_bookmark and last_modified > initial_bookmark):
                metadata = {'source': f'{self.file_dir.name}/{self.file_name}'}
                self.write_schema(
                    source=self.source,
                    # set the schema stream to the file name so the sink writes each file
                    stream=self.file_name,
                    bookmark_properties=self.bookmark_properties,
                    metadata=metadata)
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
            self.bookmarks[self.bookmark_properties[0]] = bookmark.isoformat()
            self.write_bookmarks(source=self.source, stream=self.stream)


class CsvSource(Source):
    def __init__(
            self,
            source: str,
            stream: str,
            file_dir: Path,
            key_properties: Optional[List[str]] = None,
            bookmark_properties: Optional[List[str]] = None,
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.stream = stream
        self.key_properties = key_properties
        self.bookmark_properties = bookmark_properties
        self.file_dir = file_dir
        self.file_handle: Optional[TextIO] = None

    def start(self) -> None:
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
                    self.write_schema(
                        source=self.source,
                        stream=self.stream,
                        schema=schema,
                        key_properties=self.key_properties,
                        bookmark_properties=self.bookmark_properties)
                    write_schema = False
                should_write = any([row[p] > self.bookmarks.get(p, '') for p in self.bookmark_properties])
                if should_write:
                    self.write_record({'_source': source, **row})
                    for bookmark_property in self.bookmark_properties:
                        self.bookmarks[bookmark_property] = row[bookmark_property]
            self.write_bookmarks(source=self.source, stream=self.stream)
            if not self.file_handle.closed:
                self.file_handle.close()


class LineSink(Sink):
    def __init__(
            self,
            sink: str,
            bookmarker: Bookmarker,
            temp_dir: str,
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.sink = sink
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
            elif isinstance(msg, BookmarksMessage):
                bookmarks_key = self.bookmarker.get_key(msg.source, msg.stream, self.sink)
                self.bookmarker.set_bookmarks(bookmarks_key, msg.bookmarks)
        if not self.file_handle.closed:
            self.file_handle.close()


class CsvSink(Sink):
    def __init__(self, sink: str, bookmarker: Bookmarker, temp_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.sink = sink
        self.bookmarker = bookmarker
        self.temp_dir = temp_dir
        self.source: Optional[str] = None
        self.stream: Optional[str] = None
        self.file_handle: Optional[TextIO] = None
        self.writer: Optional[csv.DictWriter] = None
        self.bookmarks_key: Optional[str] = None

    def start(self) -> None:
        for msg in self.read():
            if isinstance(msg, SchemaMessage):
                assert self.writer is None, 'writer already initialized'
                self.source = msg.source
                self.stream = msg.stream
                self.bookmarks_key = self.bookmarker.get_key(self.source, self.stream, self.sink)
                self.file_handle = Path(self.temp_dir, msg.stream).open('w', newline='')
                properties = msg.schema.get('properties', {}).keys()
                assert properties, 'schema properties must be defined'
                self.writer = csv.DictWriter(self.file_handle, fieldnames=properties)
                self.writer.writeheader()
            elif isinstance(msg, RecordMessage):
                assert self.writer is not None, 'writer is not initialized'
                self.writer.writerow(msg.record)
            elif isinstance(msg, BookmarksMessage):
                assert self.bookmarks_key is not None, 'bookmarks_key is not initialized'
                self.bookmarker.set_bookmarks(self.bookmarks_key, msg.bookmarks)
        if not self.file_handle.closed:
            self.file_handle.close()


def test_line_stream(tmpdir):
    source = 'input'
    stream = 'dogs'
    sink = 'line'
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    line_source = LineSource(
        source=source,
        stream=stream,
        file_dir=TEST_DATA_DIR / 'input',
        output_pipe=pipe)

    line_sink = LineSink(
        sink=sink,
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    line_source.start()
    pipe.seek(0)
    line_sink.start()

    actual_messages = pipe.getvalue()
    expected_messages_path = Path(TEST_DATA_DIR, 'output', 'line_messages.txt')
    expected_messages = expected_messages_path.read_text()

    assert actual_messages == expected_messages

    for file in Path(tmpdir).glob('*.csv'):
        expected_rows = Path(TEST_DATA_DIR, 'input', file.name).read_text()
        actual_rows = file.read_text()
        assert actual_rows == expected_rows

    key = bookmarker.get_key(source, stream, sink)
    actual_bookmark = bookmarker.get_bookmarks(key)
    assert actual_bookmark == {'_last_modified_at': '2020-01-02T00:00:00'}


def test_csv_stream(tmpdir):
    source = 'input'
    stream = 'dogs.csv'
    sink = 'csv'
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    csv_source = CsvSource(
        source=source,
        stream=stream,
        file_dir=TEST_DATA_DIR / 'input',
        key_properties=['id'],
        bookmark_properties=['birth_date'],
        output_pipe=pipe)

    csv_sink = CsvSink(
        sink=sink,
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    csv_source.start()
    pipe.seek(0)
    csv_sink.start()

    actual_messages = pipe.getvalue()
    expected_messages_path = Path(TEST_DATA_DIR, 'output', 'csv_messages.txt')
    expected_messages = expected_messages_path.read_text()

    assert actual_messages == expected_messages

    expected_rows = Path(TEST_DATA_DIR, 'output', 'dogs.csv').read_text()
    actual_rows = Path(tmpdir, 'dogs.csv').read_text()

    assert actual_rows == expected_rows

    key = bookmarker.get_key(source, stream, sink)
    actual_bookmarks = bookmarker.get_bookmarks(key)
    assert actual_bookmarks == {'birth_date': '2020-01-04'}


def test_csv_stream_bookmark(tmpdir):
    source = 'input'
    stream = 'dogs.csv'
    sink = 'csv'
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    key = bookmarker.get_key(source, stream, sink)
    bookmarks = {'birth_date': '2020-01-02'}

    source = CsvSource(
        source=source,
        stream=stream,
        file_dir=TEST_DATA_DIR / 'input',
        key_properties=['id'],
        bookmark_properties=['birth_date'],
        bookmarks=bookmarks,
        output_pipe=pipe)

    sink = CsvSink(
        sink=sink,
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    source.start()
    pipe.seek(0)
    sink.start()

    actual_messages = pipe.getvalue()
    expected_messages_path = Path(TEST_DATA_DIR, 'output', 'csv_messages_bookmark.txt')
    expected_messages = expected_messages_path.read_text()

    assert actual_messages == expected_messages

    expected_rows = Path(TEST_DATA_DIR, 'output', 'dogs_bookmark.csv').read_text()
    actual_rows = Path(tmpdir, 'dogs.csv').read_text()

    assert actual_rows == expected_rows

    actual_bookmarks = bookmarker.get_bookmarks(key)
    assert actual_bookmarks == {'birth_date': '2020-01-04'}


def test_csv_stream_error(tmpdir):
    bookmarker = MemoryBookmarker()
    pipe = io.StringIO()
    with Path(TEST_DATA_DIR, 'input', 'error_messages.txt').open('r') as fr:
        data = fr.read()
        pipe.write(data)
    pipe.seek(0)

    sink = LineSink(
        sink='output',
        bookmarker=bookmarker,
        temp_dir=tmpdir,
        input_pipe=pipe)

    with pytest.raises(RuntimeError):
        sink.start()
