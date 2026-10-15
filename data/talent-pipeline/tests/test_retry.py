"""Retry with exponential backoff (error-handling paths)."""

from __future__ import annotations

import pytest

import job


class Flaky:
    """Callable that fails a given number of times, then succeeds."""

    def __init__(self, failures: int, error: type[Exception] = ConnectionError) -> None:
        self.failures = failures
        self.error = error
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error(f"transient failure #{self.calls}")
        return "ok"


def test_succeeds_after_transient_failures_with_exponential_backoff() -> None:
    waits: list[float] = []
    flaky = Flaky(failures=2)
    wrapped = job.retry(attempts=3, base_delay=2.0, sleep=waits.append)(flaky)
    assert wrapped() == "ok"
    assert flaky.calls == 3
    assert waits == [2.0, 4.0]  # 2s, then doubled


def test_gives_up_after_last_attempt_and_reraises() -> None:
    waits: list[float] = []
    flaky = Flaky(failures=10)
    wrapped = job.retry(attempts=3, base_delay=1.0, sleep=waits.append)(flaky)
    with pytest.raises(ConnectionError, match="#3"):
        wrapped()
    assert flaky.calls == 3
    assert waits == [1.0, 2.0]


def test_no_retry_for_exceptions_not_listed() -> None:
    flaky = Flaky(failures=1, error=ValueError)
    wrapped = job.retry(attempts=3, exceptions=(ConnectionError,), sleep=lambda _: None)(flaky)
    with pytest.raises(ValueError):
        wrapped()
    assert flaky.calls == 1


def test_first_try_success_does_not_wait() -> None:
    waits: list[float] = []
    assert job.retry(sleep=waits.append)(Flaky(failures=0))() == "ok"
    assert waits == []


def test_retry_logs_warning_then_error(capsys: pytest.CaptureFixture[str]) -> None:
    job.setup_logging("retry-test")
    with pytest.raises(ConnectionError):
        job.retry(attempts=2, base_delay=0.5, sleep=lambda _: None)(Flaky(failures=5))()
    out = capsys.readouterr().out
    assert "WARNING" in out and "retrying in 0.5s" in out
    assert "ERROR" in out and "failed after 2 attempt(s)" in out


def test_invalid_attempts_rejected() -> None:
    with pytest.raises(ValueError):
        job.retry(attempts=0)


def test_wrapper_preserves_function_name() -> None:
    @job.retry()
    def read_source() -> None:
        """Docstring."""

    assert read_source.__name__ == "read_source"
    assert read_source.__doc__ == "Docstring."
