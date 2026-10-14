from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...models import FluentBitAgent, FluentBitAgentTopic, KafkaProvisioningOperation


class SqlAlchemyAgentRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list(self, limit: int, offset: int):
        total = self.session.query(FluentBitAgent).count()
        items = self.session.scalars(
            select(FluentBitAgent)
            .order_by(FluentBitAgent.name, FluentBitAgent.id)
            .limit(limit)
            .offset(offset)
        ).all()
        return items, total

    def get(self, agent_id: UUID, lock: bool = False):
        statement = select(FluentBitAgent).where(FluentBitAgent.id == agent_id)
        if lock:
            statement = statement.with_for_update()
        return self.session.scalar(statement)

    def topics(self, agent_id: UUID):
        return list(
            self.session.scalars(
                select(FluentBitAgentTopic)
                .where(FluentBitAgentTopic.agent_id == agent_id)
                .order_by(FluentBitAgentTopic.topic_name)
            )
        )

    def latest_operation(self, agent_id: UUID):
        return self.session.scalar(
            select(KafkaProvisioningOperation)
            .where(KafkaProvisioningOperation.agent_id == agent_id)
            .order_by(
                KafkaProvisioningOperation.created_at.desc(), KafkaProvisioningOperation.id.desc()
            )
            .limit(1)
        )

    def by_request_key(self, request_key: str):
        operation = self.session.scalar(
            select(KafkaProvisioningOperation).where(
                KafkaProvisioningOperation.request_key == request_key
            )
        )
        return (self.get(operation.agent_id), operation) if operation else None

    def add(self, agent, operation, topics):
        self.session.add(agent)
        self.session.add(operation)
        self.session.add_all(topics)
        self.session.flush()
        return agent

    def operation(self, operation_id: UUID):
        return self.session.get(KafkaProvisioningOperation, operation_id)

    def update(self, agent, values: dict):
        for key, value in values.items():
            setattr(agent, key, value)

    def claim_credentials(self, agent_id: UUID):
        agent = self.get(agent_id, lock=True)
        if agent is None or agent.state != "ready" or agent.pending_credentials_ciphertext is None:
            return None
        encrypted = agent.pending_credentials_ciphertext
        agent.pending_credentials_ciphertext = None
        self.session.flush()
        return agent, encrypted
