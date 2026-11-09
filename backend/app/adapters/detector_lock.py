"""Dedicated autocommit PostgreSQL session; never an application transaction."""
import psycopg

from ..detection.execution import LOCK_CLASS, LOCK_ID


class DetectorSessionLock:
    def __init__(self, url):
        self.url = url.replace("postgresql+psycopg:", "postgresql:")
        self.connection = None

    def acquire(self):
        self.close()
        self.connection = psycopg.connect(self.url, autocommit=True, connect_timeout=5,
            options="-c statement_timeout=5000", keepalives=1, keepalives_idle=5,
            keepalives_interval=2, keepalives_count=2, tcp_user_timeout=5000)
        if not self.connection.execute("SELECT pg_try_advisory_lock(%s,%s)", (LOCK_CLASS, LOCK_ID)).fetchone()[0]:
            self.close()
            return None
        return self.connection.info.backend_pid

    def ping(self):
        if self.connection is None:
            return False
        try:
            self.connection.execute("SELECT 1")
            return True
        except psycopg.Error:
            self.close()
            return False

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
