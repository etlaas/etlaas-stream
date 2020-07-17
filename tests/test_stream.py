import csv
import io
import pytest
from etlaas_stream import Source, Sink, SchemaMessage, RecordMessage, LineMessage
from pathlib import Path
from typing import Optional, TextIO


TEST_DATA_DIR = Path(__file__).parent / 'test_data'


class LineSource(Source):
    def __init__(self, file_dir: Path, **kwargs):
        super().__init__(**kwargs)
        self.file_dir = file_dir

        self.file_handle: Optional[TextIO] = None

    def start(self) -> None:
        for file in self.file_dir.glob('*.csv'):
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
                self.write_line(line=line.strip())
            if not self.file_handle.closed:
                self.file_handle.close()


class CsvSource(Source):
    def __init__(self, name: str, stream: str, file_dir: Path, **kwargs):
        super().__init__(name=name, stream=stream, **kwargs)
        self.file_dir = file_dir

        self.file_handle: Optional[TextIO] = None

    def start(self) -> None:
        write_schema = True
        for file in self.file_dir.glob('*.csv'):
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
            self.write_bookmark()
            if not self.file_handle.closed:
                self.file_handle.close()


class LineSink(Sink):
    def __init__(self, temp_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.temp_dir = temp_dir

        self.file_handle:  Optional[TextIO] = None

    def start(self) -> None:
        for msg in self.read():
            if isinstance(msg, SchemaMessage):
                if (self.file_handle is not None) and (not self.file_handle.closed):
                    self.file_handle.close()
                self.file_handle = Path(self.temp_dir, msg.stream).open('w')
            elif isinstance(msg, RecordMessage):
                raise ValueError(f'can only process LINE messages')
            elif isinstance(msg, LineMessage):
                assert self.file_handle is not None, 'file_handle is not initialized'
                self.file_handle.write(msg.line + '\n')
        if not self.file_handle.closed:
            self.file_handle.close()


class CsvSink(Sink):
    def __init__(self, temp_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.temp_dir = temp_dir

        self.file_handle: Optional[TextIO] = None
        self.writer: Optional[csv.DictWriter] = None

    def start(self) -> None:
        for msg in self.read():
            if isinstance(msg, SchemaMessage):
                assert self.writer is None, 'writer already initialized'
                self.file_handle = Path(self.temp_dir, self.stream).open('w', newline='')
                properties = self.schema.get('properties', {}).keys()
                assert properties, 'schema properties must be defined'
                self.writer = csv.DictWriter(self.file_handle, fieldnames=properties)
                self.writer.writeheader()
            elif isinstance(msg, RecordMessage):
                assert self.writer is not None, 'writer is not initialized'
                self.writer.writerow(msg.record)
        if not self.file_handle.closed:
            self.file_handle.close()


def test_line_stream(tmpdir):
    pipe = io.StringIO()
    source = LineSource(
        name='input',
        file_dir=TEST_DATA_DIR / 'input',
        output_pipe=pipe)

    sink = LineSink(
        name='output',
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


def test_csv_stream(tmpdir):
    pipe = io.StringIO()
    source = CsvSource(
        name='input',
        stream='dogs.csv',
        file_dir=TEST_DATA_DIR / 'input',
        key_properties=['id'],
        bookmark_properties=['birth_date'],
        output_pipe=pipe)

    sink = CsvSink(
        name='output',
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


def test_csv_stream_bookmark(tmpdir):
    pipe = io.StringIO()
    source = CsvSource(
        name='input',
        bookmark={'birth_date': '2020-01-02'},
        stream='dogs.csv',
        file_dir=TEST_DATA_DIR / 'input',
        key_properties=['id'],
        bookmark_properties=['birth_date'],
        output_pipe=pipe)

    sink = CsvSink(
        name='output',
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


def test_csv_stream_error(tmpdir):
    pipe = io.StringIO()
    with Path(TEST_DATA_DIR, 'input', 'error_messages.txt').open('r') as fr:
        data = fr.read()
        pipe.write(data)
    pipe.seek(0)

    sink = LineSink(
        name='output',
        temp_dir=tmpdir,
        input_pipe=pipe)

    with pytest.raises(RuntimeError):
        sink.start()
