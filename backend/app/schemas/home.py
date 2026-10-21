from datetime import datetime

from pydantic import BaseModel


class ConnectionCounts(BaseModel):
    own: int
    external: int


class SourceCounts(BaseModel):
    own_enabled: int
    own_disabled: int
    own_archived: int
    external_enabled: int
    external_disabled: int


class AgentCounts(BaseModel):
    provisioning: int
    ready: int
    error: int
    revocation_pending: int
    revoked: int


class HomeOverview(BaseModel):
    captured_at: datetime
    connections: ConnectionCounts | None = None
    sources: SourceCounts | None = None
    agents: AgentCounts | None = None
    normalizers: int | None = None
