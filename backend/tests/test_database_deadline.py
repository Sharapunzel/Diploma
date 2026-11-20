"""Deadline adapter boundaries; no PostgreSQL server or Docker is required."""

import socket
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Event, Thread
from time import monotonic
from types import SimpleNamespace

import psycopg
import pytest
from app import db_deadline
from app.aggregation.event_time import OperationBudget
from app.db import Database
from app.db_deadline import (
    DatabaseAddressUnavailable,
    DatabaseDeadlineExceeded,
    DeadlineConnection,
    DeadlineQueuePool,
    database_deadline,
    install_deadline_hooks,
)
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.services.implementations import aggregate_execution, aggregates
from app.services.implementations.aggregate_execution import AggregateExecutionService
from app.services.implementations.aggregates import AggregateService
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.pool import QueuePool, SingletonThreadPool


class PooledConnection:
    def rollback(self):
        pass

    def close(self):
        pass


def full_pool():
    pool = DeadlineQueuePool(PooledConnection)
    return pool, [pool.connect() for _ in range(15)]


def test_busy_pool_uses_original_budget_and_restores_capacity(record_property):
    pool, held = full_pool()
    try:
        started = monotonic()
        budget = OperationBudget(.05)
        with database_deadline(lambda: budget), pytest.raises(DatabaseDeadlineExceeded):
            pool.connect()
        elapsed = monotonic() - started
        record_property("busy_pool_release_seconds", elapsed)
        assert .04 <= elapsed < .5
        assert pool.size() == 5 and pool.timeout() == 30
        assert pool.checkedout() == 15 and pool.overflow() == 10
        assert DeadlineQueuePool._do_get is QueuePool._do_get
    finally:
        for connection in held:
            connection.close()
        assert pool.checkedout() == 0 and pool.overflow() == 0
        pool.dispose()


def test_without_scope_busy_pool_retains_ordinary_wait():
    pool, held = full_pool()
    acquired = Event()

    def borrow():
        with closing(pool.connect()):
            acquired.set()

    worker = Thread(target=borrow)
    worker.start()
    try:
        assert not acquired.wait(.08)
        held.pop().close()
        assert acquired.wait(1)
    finally:
        for connection in held:
            connection.close()
        worker.join(1)
        assert not worker.is_alive()
        assert pool.checkedout() == 0 and pool.overflow() == 0
        pool.dispose()


def test_concurrent_scopes_have_independent_original_deadlines():
    pool, held = full_pool()

    def borrow(seconds):
        budget = OperationBudget(seconds)
        started = monotonic()
        with database_deadline(lambda: budget), pytest.raises(DatabaseDeadlineExceeded):
            pool.connect()
        # The finally-reset does not leak an expired context to this thread.
        return monotonic() - started

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            short = workers.submit(borrow, .04)
            long = workers.submit(borrow, .14)
            short_elapsed, long_elapsed = short.result(1), long.result(1)
        assert .03 <= short_elapsed < .11
        assert .12 <= long_elapsed < .5
        assert long_elapsed - short_elapsed > .04
        assert pool.checkedout() == 15
    finally:
        for connection in held:
            connection.close()
        pool.dispose()


def test_finalization_replaces_expired_compute_and_scope_always_resets():
    pool = DeadlineQueuePool(PooledConnection)
    clock = [1.0]
    compute = OperationBudget(.8, clock=lambda: clock[0])
    current = [compute]
    try:
        with database_deadline(lambda: current[0]):
            clock[0] = 2.0
            with pytest.raises(DatabaseDeadlineExceeded):
                pool.connect()
            service = OperationBudget(5, clock=lambda: clock[0])
            current[0] = service
            with closing(pool.connect()):
                assert service.deadline == 7
            with database_deadline(lambda: compute), pytest.raises(DatabaseDeadlineExceeded):
                pool.connect()
            with closing(pool.connect()):
                pass  # Restored service context, not min(expired T, service5).
        clock[0] = 100
        with closing(pool.connect()):
            pass  # No expired context remains outside the scope.
    finally:
        assert pool.checkedout() == 0
        pool.dispose()


def make_engine():
    engine = create_engine("postgresql+psycopg://user:password@db.example/test",
        pool_pre_ping=True, poolclass=DeadlineQueuePool)
    install_deadline_hooks(engine)
    return engine


def connect_hook(engine):
    return next(iter(engine.dialect.dispatch.do_connect))


def test_unknown_scoped_name_is_refused_before_dns_and_not_a_hard_timeout(monkeypatch):
    engine = make_engine()
    calls = []
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: calls.append("dns"))
    try:
        budget = OperationBudget(1)
        with database_deadline(lambda: budget), pytest.raises(DatabaseAddressUnavailable):
            connect_hook(engine)(engine.dialect, None, [], {"host": "db.example"})
        assert calls == []
        assert budget.remaining() > 0
    finally:
        engine.dispose()


def test_successful_engine_address_is_local_copied_and_preserves_tls_host(monkeypatch):
    first, second = make_engine(), make_engine()
    calls = []
    connection = SimpleNamespace(info=SimpleNamespace(host="db.example", port=5432, hostaddr="127.0.0.1"))

    def connect(*args, **kwargs):
        calls.append(kwargs)
        return connection

    monkeypatch.setattr(DeadlineConnection, "connect", connect)
    parameters = {"host": "db.example", "port": 5432, "sslmode": "verify-full"}
    original = dict(parameters)
    try:
        connect_hook(first)(first.dialect, None, [], parameters)
        assert calls == [original]
        budget = OperationBudget(.8)
        with database_deadline(lambda: budget):
            connect_hook(first)(first.dialect, None, [], parameters)
            with pytest.raises(DatabaseAddressUnavailable):
                connect_hook(second)(second.dialect, None, [], parameters)
        assert calls[-1] == {**original, "hostaddr": "127.0.0.1"}
        assert parameters == original
    finally:
        first.dispose()
        second.dispose()


def test_explicit_multihost_addresses_and_driver_params_do_not_mutate_neighbors():
    original = {"host": "one.example,two.example", "hostaddr": "127.0.0.1,127.0.0.2",
        "connect_timeout": "30"}
    budget = OperationBudget(.8)
    with database_deadline(lambda: budget):
        scoped = DeadlineConnection._get_connection_params("", **original)
    ordinary = DeadlineConnection._get_connection_params("", **original)
    assert scoped == {**original, "connect_timeout": 2}
    assert ordinary == original


def test_multihost_retries_consume_one_budget_and_close_incomplete_connect(monkeypatch):
    clock = [0.0]
    budget = OperationBudget(.8, clock=lambda: clock[0])
    starts, closes = [], []

    def native(_cls, conninfo):
        starts.append(conninfo)
        try:
            yield 1, 1
        finally:
            closes.append(conninfo)

    monkeypatch.setattr(psycopg.Connection, "_connect_gen", classmethod(native))
    with database_deadline(lambda: budget):
        first = DeadlineConnection._connect_gen("first")
        assert next(first) == (1, 1)
        clock[0] = .7
        second = DeadlineConnection._connect_gen("second")
        assert next(second) == (1, 1)
        clock[0] = .81
        for generator in (first, second):
            with pytest.raises(DatabaseDeadlineExceeded):
                generator.send(0)
    assert starts == ["first", "second"]
    assert closes == ["first", "second"]
    assert budget.deadline == .8


@pytest.mark.parametrize("addresses", [1, 2])
def test_real_silent_connect_releases_socket_and_pool_without_dns(monkeypatch, record_property, addresses):
    peer_closed = Event()
    seconds = .05 if addresses == 1 else 2.4
    with closing(socket.socket()) as listener, closing(socket.socket()) as refused:
        listener.bind(("127.0.0.1", 0))
        refused.bind(("127.0.0.1", 0))
        refused.listen(1)
        refused.settimeout(seconds + 1)
        listener.listen(1)
        listener.settimeout(seconds + 1)

        def server():
            if addresses == 2:
                # Reject the first numeric endpoint immediately. A bound,
                # non-listening socket can silently drop SYNs on Windows.
                with closing(refused.accept()[0]):
                    pass
            with closing(listener.accept()[0]) as peer:
                peer.settimeout(seconds + 1)
                try:
                    while peer.recv(4096):
                        pass
                except ConnectionResetError:
                    pass
                peer_closed.set()

        worker = Thread(target=server)
        worker.start()
        port = listener.getsockname()[1]
        parameters = {} if addresses == 1 else {"host": "first.example,second.example",
            "hostaddr": "127.0.0.1,127.0.0.1", "port": f"{refused.getsockname()[1]},{port}"}
        engine = create_engine(f"postgresql+psycopg://user:password@127.0.0.1:{port}/test",
            pool_pre_ping=True, poolclass=DeadlineQueuePool, connect_args=parameters)
        install_deadline_hooks(engine)
        monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: pytest.fail("unexpected DNS"))
        budget = OperationBudget(seconds)
        deadline = budget.deadline
        started = monotonic()
        try:
            with database_deadline(lambda: budget), pytest.raises(Exception) as caught:
                engine.connect()
            # The Engine wraps a driver error; the origin remains explicit.
            assert isinstance(getattr(caught.value, "orig", caught.value), DatabaseDeadlineExceeded)
            assert peer_closed.wait(.5)
            elapsed = monotonic() - started
            record_property("silent_connect_release_seconds", elapsed)
            assert seconds - .02 <= elapsed < seconds + .7
            assert budget.deadline == deadline
            assert engine.pool.checkedout() == 0 and engine.pool.overflow() == -5
        finally:
            engine.dispose()
            worker.join(1)
            assert not worker.is_alive()


def test_pre_ping_driver_wait_has_same_remaining_and_discards_expired_connection(monkeypatch):
    clock = [0.0]
    budget = OperationBudget(.8, clock=lambda: clock[0])
    finished, waits, finalization, cancelled = [], [], [], []
    pgconn = SimpleNamespace(finish=lambda: finished.append(True), status=psycopg.pq.ConnStatus.BAD, socket=0,
        transaction_status=psycopg.pq.TransactionStatus.ACTIVE)
    connection = DeadlineConnection(pgconn)
    connection._autocommit = True

    class Cursor:
        def execute(self, _statement):
            connection.wait(iter(()))

        def close(self):
            pass

    connection.cursor = Cursor

    def native_wait(_generator, _socket, *, interval, timeout):
        waits.append(timeout)
        clock[0] = .81
        raise psycopg.errors._WaitTimeout()

    def service_budget():
        if not finalization:
            finalization.append(OperationBudget(5, clock=lambda: clock[0]))
        return finalization[0]

    def cancel(*, timeout):
        cancelled.append(timeout)
        clock[0] += .2

    monkeypatch.setattr(psycopg.waiting, "wait", native_wait)
    monkeypatch.setattr(connection, "cancel_safe", cancel)
    with database_deadline(lambda: budget, on_deadline=service_budget), pytest.raises(DatabaseDeadlineExceeded):
        PGDialect_psycopg().do_ping(connection)
    assert waits == [.8] and finished == [True]
    assert cancelled == [5] and len(finalization) == 1
    assert service_budget() is finalization[0] and finalization[0].deadline == pytest.approx(5.81)
    assert finalization[0].remaining() == pytest.approx(4.8)
    # Prevent destructor diagnostics from the deliberately minimal PGconn stub.
    connection._closed = True


def test_without_scope_driver_wait_retains_original_timeout_behavior(monkeypatch):
    finished, waits = [], []
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: finished.append(True),
        status=psycopg.pq.ConnStatus.BAD))

    def native_wait(_connection, _generator, *, interval, timeout):
        waits.append(timeout)
        raise psycopg.errors._WaitTimeout()

    monkeypatch.setattr(psycopg.Connection, "wait", native_wait)
    with pytest.raises(psycopg.errors._WaitTimeout):
        connection.wait(iter(()), timeout=.1)
    assert waits == [.1] and finished == []
    connection._closed = True


def test_without_scope_non_postgres_database_keeps_its_default_pool():
    database = Database("sqlite://")
    try:
        assert type(database.engine.pool) is SingletonThreadPool
    finally:
        database.dispose()


def test_older_driver_without_wait_timeout_fails_closed_only_in_scope(monkeypatch):
    finished = []
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: finished.append(True),
        status=psycopg.pq.ConnStatus.BAD))
    monkeypatch.setattr(db_deadline, "_WAIT_TIMEOUT_SUPPORTED", False)
    budget = OperationBudget(.8)
    with database_deadline(lambda: budget), pytest.raises(DatabaseAddressUnavailable):
        connection.wait(iter(()))
    assert finished == [True]
    connection._closed = True


@pytest.mark.parametrize("unsupported", ["native_cancel", "missing_service_budget"])
def test_unbounded_cancellation_fails_closed_before_subject_sql(monkeypatch, unsupported):
    finished, started = [], []
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: finished.append(True),
        status=psycopg.pq.ConnStatus.BAD))
    monkeypatch.setattr(psycopg.waiting, "wait", lambda *args, **kwargs: started.append("sql"))
    monkeypatch.setattr(connection, "cancel", lambda: started.append("blocking_cancel"))
    if unsupported == "native_cancel":
        monkeypatch.setattr(db_deadline, "_CANCEL_SAFE_SUPPORTED", False)
    budget = OperationBudget(.8)
    service = OperationBudget(5)
    callback = (lambda: service) if unsupported == "native_cancel" else None
    with database_deadline(lambda: budget, on_deadline=callback), pytest.raises(DatabaseAddressUnavailable):
        connection.wait(iter(()))
    assert started == [] and finished == [True]
    connection._closed = True


def test_exhausted_service_budget_is_never_renewed_for_protocol_cancel(monkeypatch):
    clock = [0.0]
    service = OperationBudget(5, clock=lambda: clock[0])
    cancelled, finished = [], []
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: finished.append(True),
        status=psycopg.pq.ConnStatus.BAD, transaction_status=psycopg.pq.TransactionStatus.ACTIVE))
    monkeypatch.setattr(connection, "cancel_safe", lambda **kwargs: cancelled.append(kwargs))
    clock[0] = 5.1
    with database_deadline(lambda: service, on_deadline=lambda: service), pytest.raises(DatabaseDeadlineExceeded):
        connection.wait(iter(()))
    assert cancelled == [] and finished == [True] and service.deadline == 5
    connection._closed = True


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit()])
def test_interrupt_keeps_original_exception_and_one_shared_cancel_budget(monkeypatch, error):
    clock = [0.0]
    budget = OperationBudget(.8, clock=lambda: clock[0])
    service, cancelled, finished = [], [], []
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: finished.append(True), socket=0,
        status=psycopg.pq.ConnStatus.BAD, transaction_status=psycopg.pq.TransactionStatus.ACTIVE))

    def finalize():
        if not service:
            service.append(OperationBudget(5, clock=lambda: clock[0]))
        return service[0]

    def wait(*_args, **_kwargs):
        raise error

    def cancel(*, timeout):
        cancelled.append(timeout)
        clock[0] += .2

    monkeypatch.setattr(psycopg.waiting, "wait", wait)
    monkeypatch.setattr(connection, "cancel_safe", cancel)
    with database_deadline(lambda: budget, on_deadline=finalize), pytest.raises(type(error)) as failure:
        connection.wait(iter(()))
    assert failure.value is error and finished == [True] and cancelled == [5]
    assert len(service) == 1 and finalize() is service[0] and service[0].deadline == 5
    assert service[0].remaining() == pytest.approx(4.8)
    connection._closed = True


def test_unconfirmed_protocol_cancel_still_closes_and_preserves_deadline_error(monkeypatch):
    clock = [0.0]
    budget = OperationBudget(.8, clock=lambda: clock[0])
    service = OperationBudget(5, clock=lambda: clock[0])
    finished = []
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: finished.append(True), socket=0,
        status=psycopg.pq.ConnStatus.BAD, transaction_status=psycopg.pq.TransactionStatus.ACTIVE))

    def wait(*_args, **_kwargs):
        clock[0] = .81
        raise psycopg.errors._WaitTimeout()

    def cancel(**_kwargs):
        raise psycopg.errors.CancellationTimeout()

    monkeypatch.setattr(psycopg.waiting, "wait", wait)
    monkeypatch.setattr(connection, "cancel_safe", cancel)
    with database_deadline(lambda: budget, on_deadline=lambda: service), pytest.raises(DatabaseDeadlineExceeded):
        connection.wait(iter(()))
    assert finished == [True] and service.deadline == 5
    connection._closed = True


@pytest.mark.parametrize("entrypoint", ["advance", "preview"])
def test_cancel_allocates_service_budget_without_renewing_failed_checkout(monkeypatch, entrypoint):
    clock = [0.0]
    compute = OperationBudget(.8, clock=lambda: clock[0])
    cancelled = []
    uow = SimpleNamespace(rollback=lambda: None)
    if entrypoint == "advance":
        service = AggregateExecutionService(None, uow, None, None)
        module, method = aggregate_execution, "_advance"
    else:
        service = AggregateService(None, None, None, None, uow, None, detectors=None)
        module, method = aggregates, "_preview_calculate"
    monkeypatch.setattr(module, "OperationBudget", lambda seconds: OperationBudget(seconds, clock=lambda: clock[0]))
    connection = DeadlineConnection(SimpleNamespace(finish=lambda: None, socket=0,
        status=psycopg.pq.ConnStatus.BAD, transaction_status=psycopg.pq.TransactionStatus.ACTIVE))

    def wait(*_args, **_kwargs):
        clock[0] = .81
        raise psycopg.errors._WaitTimeout()

    def calculate(_argument, **_kwargs):
        service.current_operation_budget = compute
        try:
            connection.wait(iter(()))
        except DatabaseDeadlineExceeded:
            assert service.current_operation_budget is compute
            # This models SQLAlchemy pre_ping's automatic reconnect after a
            # disconnect. Even though cancel has a service budget, no fresh
            # connection/initialization/subject SQL may start in this checkout.
            with pytest.raises(DatabaseDeadlineExceeded):
                DeadlineConnection._get_connection_params("", host="127.0.0.1")
            raise

    def cancel(**kwargs):
        cancelled.append(kwargs["timeout"])
        clock[0] += .2

    monkeypatch.setattr(psycopg.waiting, "wait", wait)
    monkeypatch.setattr(connection, "cancel_safe", cancel)
    monkeypatch.setattr(service, method, calculate)
    with pytest.raises(DatabaseDeadlineExceeded):
        (service.advance if entrypoint == "advance" else service._preview)(None)
    assert cancelled == [5]
    assert service.current_operation_budget is not compute
    assert service.current_operation_budget.deadline == pytest.approx(5.81)
    assert service.current_operation_budget.remaining() == pytest.approx(4.8)
    connection._closed = True


def test_expired_checkout_and_sql_hooks_do_not_start_subject_sql():
    engine = make_engine()
    clock = [0.0]
    budget = OperationBudget(.8, clock=lambda: clock[0])
    invalidated = []
    connection = SimpleNamespace(invalidate=invalidated.append)
    try:
        with database_deadline(lambda: budget):
            clock[0] = 1
            with pytest.raises(DatabaseDeadlineExceeded):
                engine.pool.dispatch.checkout(None, None, None)
            with pytest.raises(DatabaseDeadlineExceeded):
                engine.dispatch.before_cursor_execute(connection, None, "SELECT forbidden", (), None, False)
        assert len(invalidated) == 1 and isinstance(invalidated[0], DatabaseDeadlineExceeded)
    finally:
        engine.dispose()


@pytest.mark.parametrize("entrypoint", ["advance", "preview"])
def test_calculation_scope_already_covers_rr_set_transaction_checkout(monkeypatch, entrypoint):
    pool, held = full_pool()
    statements, entered = [], []

    class Session:
        def rollback(self):
            pass

        def execute(self, statement):
            statements.append(str(statement))
            pool.connect()  # The actual UoW's first RR SQL acquires a connection.

    uow = SqlAlchemyUnitOfWork(Session())
    if entrypoint == "advance":
        service = AggregateExecutionService(None, uow, None, None)
        original = "_advance"
    else:
        service = AggregateService(None, None, None, None, uow, None, detectors=None)
        original = "_preview_calculate"

    def calculation(_argument, **_kwargs):
        service.current_operation_budget = OperationBudget(.04)
        with uow.read_snapshot():
            entered.append(True)

    monkeypatch.setattr(service, original, calculation)
    try:
        with pytest.raises(DatabaseDeadlineExceeded):
            (service.advance if entrypoint == "advance" else service._preview)(None)
        assert statements == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"]
        assert entered == [] and pool.checkedout() == 15
        assert service.current_operation_budget.seconds == 5
    finally:
        for connection in held:
            connection.close()
        # Finally-reset restores normal checkout even after the failed RR.
        with closing(pool.connect()):
            pass
        pool.dispose()
