import logging
import time

logger = logging.getLogger(__name__)


class CircuitBreakerOpen(Exception):
    pass


class CircuitBreaker:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failure_count = 0
        self._opened_at: float = 0.0
        self._state = self.CLOSED

    @property
    def state(self) -> str:
        if self._state == self.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = self.HALF_OPEN
        return self._state

    def call(self, func, *args, **kwargs):
        state = self.state
        if state == self.OPEN:
            raise CircuitBreakerOpen(f"Circuit {self.name!r} is OPEN")
        try:
            result = func(*args, **kwargs)
            if state == self.HALF_OPEN:
                self._reset()
            return result
        except CircuitBreakerOpen:
            raise
        except Exception:
            self._on_failure()
            raise

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._opened_at = time.monotonic()
        if self._failure_count >= self.failure_threshold:
            if self._state != self.OPEN:
                logger.warning("Circuit %r opened after %d consecutive failures", self.name, self._failure_count)
            self._state = self.OPEN

    def _reset(self) -> None:
        self._failure_count = 0
        self._opened_at = 0.0
        self._state = self.CLOSED
        logger.info("Circuit %r closed after successful probe", self.name)
