"""Attempt-local deadlines for the existing synchronous SQLAlchemy pool.

Only QueuePool's queue and Psycopg's documented wait boundary are adapted;
the pool's checkout/overflow/reconnect algorithms remain SQLAlchemy's. The
private extension points (_queue_class and _connect_gen) are isolated here.
Scoped connects accept numeric addresses or this Engine's last confirmed
single-host address. Unknown DNS paths fail closed before calling a resolver;
unscoped connections keep their ordinary hostname resolution and defaults.
"""

from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from inspect import signature
from ipaddress import ip_address
from logging import getLogger
from math import ceil, isfinite
from threading import Lock
from time import monotonic
from typing import Protocol

import psycopg
from psycopg import capabilities
from psycopg.errors import _WaitTimeout
from sqlalchemy import event
from sqlalchemy.pool import QueuePool
from sqlalchemy.util.queue import Empty, Queue

_WAIT_TIMEOUT_SUPPORTED = "timeout" in signature(psycopg.Connection.wait).parameters
_CANCEL_SAFE_SUPPORTED = capabilities.has_cancel_safe()
_LOGGER = getLogger(__name__)


class DeadlineBudget(Protocol):
    def remaining(self) -> float: ...


class DatabaseDeadlineExceeded(psycopg.OperationalError):
    """The active operation's original deadline has actually expired."""


class DatabaseAddressUnavailable(psycopg.OperationalError):
    """A scoped connection cannot guarantee a required I/O bound."""


_current: ContextVar[Callable[[], DeadlineBudget | None] | None] = ContextVar(
    "database_operation_budget", default=None)
_finalization: ContextVar[Callable[[], DeadlineBudget] | None] = ContextVar(
    "database_finalization_budget", default=None)


@contextmanager
def database_deadline(budget: Callable[[], DeadlineBudget | None], *,
        on_deadline: Callable[[], DeadlineBudget] | None = None):
    """Bind the actual budget, allowing an explicit finalization replacement.

    No fresh duration is allocated and no minimum with an expired compute
    context is taken. ContextVar tokens restore the caller even on failure.
    """
    token = _current.set(budget)
    finalization_token = _finalization.set(on_deadline)
    try:
        yield
    finally:
        _finalization.reset(finalization_token)
        _current.reset(token)


def _remaining():
    provider = _current.get()
    budget = provider() if provider is not None else None
    if budget is None:
        return None
    remaining = budget.remaining()
    if not isfinite(remaining) or remaining <= 0:
        raise DatabaseDeadlineExceeded("database operation deadline exhausted")
    return remaining


class DeadlineQueue(Queue):
    def get(self, block=True, timeout=None):
        remaining = _remaining()
        if remaining is None:
            return super().get(block, timeout)
        queue_deadline = monotonic() + timeout if timeout is not None else None
        while True:
            remaining = _remaining()
            if queue_deadline is not None:
                queue_remaining = queue_deadline - monotonic()
                if queue_remaining <= 0:
                    raise Empty
                remaining = min(remaining, queue_remaining)
            try:
                # Do not check after removing an entry: checkout owns cleanup
                # only once QueuePool has received that entry.
                return super().get(block, remaining)
            except Empty:
                _remaining()
                if not block or queue_deadline is not None and monotonic() >= queue_deadline:
                    raise
                # Some condition waits return just before their requested
                # timeout. Consume only the same absolute remainder, never a
                # renewed pool timeout or a falsely confirmed deadline error.


class DeadlineQueuePool(QueuePool):
    _queue_class = DeadlineQueue


class DeadlineConnection(psycopg.Connection):
    @classmethod
    def _get_connection_params(cls, conninfo, **kwargs):
        parameters = super()._get_connection_params(conninfo, **kwargs)
        remaining = _remaining()
        if remaining is not None:
            requested = ceil(remaining)
            configured = parameters.get("connect_timeout")
            if configured is not None and float(configured) > 0:
                requested = min(requested, ceil(float(configured)))
            # Psycopg 3.3.6's native connect_timeout minimum is two seconds.
            # The generator additionally checks the original subsecond budget.
            parameters = {**parameters, "connect_timeout": max(2, requested)}
        return parameters

    @classmethod
    def _connect_gen(cls, conninfo=""):
        inner = super()._connect_gen(conninfo)
        try:
            _remaining()  # Also runs after synchronous DNS/address resolution.
            request = next(inner)
            while True:
                ready = yield request
                _remaining()  # Psycopg wakes periodically even without I/O.
                request = inner.send(ready)
        except StopIteration as complete:
            connection = complete.value
            try:
                _remaining()
            except DatabaseDeadlineExceeded:
                connection.close()
                raise
            return connection
        finally:
            # Closing the delegated generator releases an unfinished PGconn.
            inner.close()

    def wait(self, gen, interval=0.1, timeout=None):
        try:
            remaining = _remaining()
        except DatabaseDeadlineExceeded:
            self._cancel_and_close(_finalization.get())
            raise
        if remaining is None:
            if timeout is None:
                return super().wait(gen, interval=interval)
            return super().wait(gen, interval=interval, timeout=timeout)
        if not _WAIT_TIMEOUT_SUPPORTED or not _CANCEL_SAFE_SUPPORTED or _finalization.get() is None:
            self.pgconn.finish()
            raise DatabaseAddressUnavailable("bounded database cancellation unavailable")
        try:
            timeout = remaining if timeout is None else min(timeout, remaining)
            # Same native wait primitive, without Connection.wait's independent
            # interrupt cancel5 + drain5 allowances. This scope owns one shared
            # finalization budget for every cancellation path.
            return psycopg.waiting.wait(gen, self.pgconn.socket, interval=interval, timeout=timeout)
        except _WaitTimeout:
            try:
                remaining = _remaining()
            except DatabaseDeadlineExceeded:
                self._cancel_and_close(_finalization.get())
                raise
            else:
                # An explicitly shorter driver timeout is not proof of the
                # operation deadline. Cancel within its existing compute budget.
                self._cancel_and_close(_current.get())
                raise
        except DatabaseDeadlineExceeded:
            self._cancel_and_close(_finalization.get())
            raise
        except (KeyboardInterrupt, SystemExit):
            self._cancel_and_close(_finalization.get())
            raise

    def _cancel_and_close(self, budget_provider):
        try:
            if (_CANCEL_SAFE_SUPPORTED and budget_provider is not None
                    and self.pgconn.transaction_status == psycopg.pq.TransactionStatus.ACTIVE):
                budget = budget_provider() if budget_provider is not None else None
                remaining = budget.remaining() if budget is not None else 0
                if isfinite(remaining) and remaining > 0:
                    # Guarded above: never invoke cancel_safe's old-libpq
                    # unbounded cancel() fallback. This is protocol cancellation,
                    # not another SQL phase or a new budget allocated by DB.
                    self.cancel_safe(timeout=remaining)
        except (psycopg.Error, OSError) as error:
            _LOGGER.warning("database_protocol_cancel_unconfirmed", extra={"error_type": type(error).__name__})
        finally:
            # SQLAlchemy invalidates the closed connection. A sent COMMIT still
            # has an unknown outcome and must be resolved by the service.
            self.pgconn.finish()


def install_deadline_hooks(engine):
    if engine.dialect.name == "postgresql" and engine.dialect.driver == "psycopg":
        # One successful server address for this Engine's single-host URL.
        # No password/URL mutation or global resolver cache; multihost scoped
        # connects require explicit numeric hostaddr for every configured host.
        resolved = None
        address_lock = Lock()

        @event.listens_for(engine, "do_connect")
        def connect(_dialect, _record, arguments, parameters):
            nonlocal resolved
            scoped = _remaining() is not None
            copied = dict(parameters)
            if scoped:
                parsed = psycopg.conninfo.conninfo_to_dict(arguments[0] if arguments else "", **copied)
                host, port = parsed.get("host", ""), str(parsed.get("port", "5432"))
                addresses = parsed.get("hostaddr")
                if addresses is not None:
                    try:
                        for address in addresses.split(","):
                            ip_address(address)
                    except ValueError as error:
                        raise DatabaseAddressUnavailable("numeric database hostaddr required") from error
                elif host and not host.startswith("/"):
                    try:
                        ip_address(host)
                        addresses = host
                    except ValueError:
                        with address_lock:
                            known = resolved
                        if known is None or known[:2] != (host, port) or "," in host:
                            raise DatabaseAddressUnavailable("bounded database address unavailable") from None
                        addresses = known[2]
                    copied["hostaddr"] = addresses
                elif not host:
                    # Implicit PGHOST/service/default-host resolution is not
                    # bounded. Ordinary, unscoped application reads retain it.
                    raise DatabaseAddressUnavailable("explicit database address required")
                _remaining()
            connection = DeadlineConnection.connect(*arguments, **copied)
            if not scoped and connection.info.hostaddr:
                with address_lock:
                    resolved = (connection.info.host, str(connection.info.port), connection.info.hostaddr)
            return connection

    @event.listens_for(engine.pool, "checkout")
    def checkout(_connection, _record, _proxy):
        _remaining()  # Runs after connect/initialization/pre_ping/reconnect.

    @event.listens_for(engine, "before_cursor_execute")
    def before_sql(connection, _cursor, _statement, _parameters, _context, _many):
        try:
            _remaining()
        except DatabaseDeadlineExceeded as error:
            connection.invalidate(error)
            raise
