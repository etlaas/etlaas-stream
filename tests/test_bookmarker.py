from etlaas_stream import MemoryBookmarker


def test_bookmarker():
    source = 'input'
    stream = 'test'
    sink = 'output'

    bookmarker = MemoryBookmarker()
    key = bookmarker.get_key(source, stream, sink)
    expected_bookmarks = {'a': 1, 'b': 2}
    bookmarker.set_bookmarks(key=key, bookmarks=expected_bookmarks)

    actual_bookmarks = bookmarker.get_bookmarks(key=key)

    assert actual_bookmarks == expected_bookmarks
