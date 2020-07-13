# Stream
Convenience classes for working with streams of data.

## Setup
1. Install Python.
2. Install Poetry.
3. Install requirements.
    ```
    poetry install --dev
    ```

## Testing
Run the type checker.
```
poetry run mypy . --strict
```

Run the unit tests.
```
poetry run pytest -v /tests
```

## Usage
See [test_stream.py](./tests/test_stream.py) for examples of how
to use the `Source` and `Sink` classes.
