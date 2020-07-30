from etlaas_stream import MemoryBookmarker


def test_bookmarker():
    source = 'input'
    stream = 'test'
    sink = 'output'

    bookmarker = MemoryBookmarker()
    bookmark = {'a': 1, 'b': 2}
    key = bookmarker.create_key(source, sink, stream)
    bookmarker.set_bookmark(key=key, value=bookmark)

    retrieved_bookmark = bookmarker.get_bookmark(key=key)

    assert retrieved_bookmark == bookmark
