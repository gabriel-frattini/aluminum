class Result:
    _results: list[dict]

    def __init__(self, _results: list[dict]) -> None:
        self._results = _results

    def all(self) -> list[dict]:  # noqa: D102,A003
        return self._results
