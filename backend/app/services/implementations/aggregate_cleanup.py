"""Durable commands and restartable cleanup on the existing aggregate worker."""

from uuid import uuid4

from sqlalchemy.exc import DBAPIError

from ...aggregation.event_time import EventTimeBudgetError, OperationBudget
from ...core.errors import DomainError
from ...detection.contracts import digest
from ...schemas.aggregates import AggregateCommandReceipt, CleanupOperation, CleanupProtection
from .aggregate_protection import AggregateProtectionService


class AggregateCleanupService:
    def __init__(self, repository, uow):
        self.repository, self.uow = repository, uow

    def _target(self, aggregate_id, budget, lock=False):
        self.repository.read_budget(budget.milliseconds())
        aggregate = self.repository.get(aggregate_id)
        if aggregate is None:
            raise DomainError("aggregate_not_found", "Агрегат не найден", 404)
        self.repository.read_budget(min(1000, budget.milliseconds()) if lock else budget.milliseconds())
        schedule = self.repository.schedule(aggregate.schedule_id, lock)
        if lock:
            self.repository.refresh(aggregate)
        return aggregate, schedule

    def preview(self, aggregate_id, keep_last):
        budget = OperationBudget(30)
        try:
            with self.uow.read_snapshot():
                aggregate, schedule = self._target(aggregate_id, budget)
                detectors = self.repository.protection_detectors(aggregate_id, budget)
                plan = AggregateProtectionService(self.repository).plan(aggregate, schedule, detectors, keep_last, budget)
                if schedule.active_cleanup_id:
                    plan.update(can_cleanup=False, reason="cleanup_in_progress")
                return CleanupProtection(**plan)
        except EventTimeBudgetError as error:
            raise DomainError("cleanup_protection_unavailable", "Не удалось проверить все зависимости очистки", 409,
                {"factor": error.factor}) from error

    def command(self, aggregate_id, actor, key, action, keep_last=None, operation_id=None):
        if not isinstance(key, str) or not key.strip() or len(key) > 200:
            raise DomainError("invalid_idempotency_key", "Укажите Idempotency-Key до 200 символов", 422)
        budget = OperationBudget(30)
        fingerprint = digest({"target": str(aggregate_id), "keep_last": keep_last,
            "operation_id": str(operation_id) if operation_id else None})
        try:
            previous = self.repository.aggregate_command(actor, action, key, budget)
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise DomainError("idempotency_conflict", "Ключ уже использован для другого запроса", 409)
                receipt = AggregateCommandReceipt(**previous["receipt"])
                self.uow.rollback()
                return receipt
            aggregate, schedule = self._target(aggregate_id, budget, True)
            if schedule.active_cleanup_id and action not in {"cleanup_retry"}:
                raise DomainError("cleanup_in_progress", "Очистка ещё не завершена", 409)
            if action in {"pause", "unpause"} and aggregate.execution_mode == "retired":
                raise DomainError("aggregate_time_semantics_retired", "Прежний расчёт остановлен; создайте копию", 409)
            if action in {"pause", "unpause"} and aggregate.state != "active":
                raise DomainError("aggregate_inactive", "Для команды требуется активный агрегат", 409)
            detectors = self.repository.protection_detectors(aggregate_id, budget, lock=True)
            cleanup_id = None
            if action == "cleanup":
                # All checks and the accepted boundary are serialized with
                # create/start/stop by schedule -> stable detector locks.
                plan = AggregateProtectionService(self.repository).plan(aggregate, schedule, detectors, keep_last, budget)
                if not plan["can_cleanup"]:
                    raise DomainError(plan["reason"], "Очистка заблокирована зависимыми детекторами", 409,
                        CleanupProtection(**plan).model_dump(mode="json"))
                cleanup_id = uuid4()
                self.repository.read_budget(budget.milliseconds())
                cutoff = self.repository.cleanup_tail(aggregate_id, keep_last) if plan["planned_delete"] else None
                self.repository.add_cleanup_operation({"id": cleanup_id, "aggregate_id": aggregate_id,
                    "created_at": self.repository.now(), "requested_keep": keep_last,
                    "reference_tail": plan["tail_identity"], "cutoff": cutoff,
                    "planned_delete": plan["planned_delete"], "deleted_points": 0,
                    "deleted_bytes_if_measured": None, "completed_at": None, "status": "pending",
                    "safe_error": None, "progress": {}}, budget)
                schedule.active_cleanup_id = cleanup_id
            elif action == "cleanup_retry":
                operation = self.repository.cleanup_operation(aggregate_id, operation_id, True)
                if operation is None:
                    raise DomainError("cleanup_operation_not_found", "Операция не найдена", 404)
                if operation.status != "failed" or schedule.active_cleanup_id != operation_id:
                    raise DomainError("cleanup_retry_unavailable", "Операция не ожидает повторного запуска", 409)
                operation.status, operation.safe_error = "pending", None
                cleanup_id = operation_id
            elif action in {"pause", "unpause"}:
                was_paused = schedule.paused
                schedule.paused = action == "pause"
                if action == "unpause" and was_paused:
                    self.repository.record_aggregate_resume(aggregate, schedule, detectors, budget)
            else:
                raise DomainError("invalid_aggregate_command", "Неизвестная команда", 422)
            schedule.execution_generation += 1
            receipt = AggregateCommandReceipt(operation_id=uuid4(), aggregate_id=aggregate_id, action=action,
                recorded_at=self.repository.now(), execution_generation=schedule.execution_generation,
                cleanup_operation_id=cleanup_id)
            self.repository.save_aggregate_command({"id": receipt.operation_id, "actor_id": actor,
                "aggregate_id": aggregate_id, "action": action, "key": key, "fingerprint": fingerprint,
                "receipt": receipt.model_dump(mode="json")}, budget)
            self.repository.read_budget(budget.milliseconds())
            self.uow.commit()
            return receipt
        except EventTimeBudgetError as error:
            self.uow.rollback()
            raise DomainError("cleanup_protection_unavailable", "Не удалось проверить все зависимости очистки", 409,
                {"factor": error.factor}) from error
        except DBAPIError as error:
            self.uow.rollback()
            raise DomainError("aggregate_command_unconfirmed", "Результат команды требует проверки; повторите тот же ключ", 503,
                {"outcome": "unknown", "resolution": "retry_same_key"}) from error
        except Exception:
            self.uow.rollback()
            raise

    def operation(self, aggregate_id, operation_id):
        with self.uow.read_snapshot():
            self._target(aggregate_id, OperationBudget(5))
            operation = self.repository.cleanup_operation(aggregate_id, operation_id)
            if operation is None:
                raise DomainError("cleanup_operation_not_found", "Операция не найдена", 404)
            return CleanupOperation.model_validate(operation, from_attributes=True)

    def advance(self, aggregate_id, operation_id):
        budget = OperationBudget(5)
        try:
            _aggregate, schedule = self._target(aggregate_id, budget, True)
            operation = self.repository.cleanup_operation(aggregate_id, operation_id, True)
            if operation is None or schedule.active_cleanup_id != operation_id:
                self.uow.rollback()
                return {"state": "cleanup_cancelled"}
            if operation.status in {"failed", "succeeded"}:
                self.uow.rollback()
                return {"state": operation.status}
            operation.status = "running"
            points = self.repository.cleanup_batch(aggregate_id, operation.cutoff, budget) if operation.cutoff else []
            # One bounded dependency portion per turn leaves enough time for
            # its atomic progress commit and fairly serves other aggregates.
            if points:
                closure = points[0]
                if self.repository.compact_point(closure, budget):
                    operation.deleted_points += 1
                operation.progress = {"origin_point_id": str(closure.origin_point_id)}
            else:
                operation.status, operation.completed_at = "succeeded", self.repository.now()
                schedule.active_cleanup_id = None
            self.repository.read_budget(min(1000, budget.milliseconds()))
            self.uow.commit()
            return {"state": operation.status, "deleted_points": operation.deleted_points}
        except Exception:
            self.uow.rollback()
            # A failed operation retains both its boundary and execution fence.
            failure_budget = OperationBudget(5)
            _, schedule = self._target(aggregate_id, failure_budget, True)
            operation = self.repository.cleanup_operation(aggregate_id, operation_id, True)
            if operation and schedule.active_cleanup_id == operation_id and operation.status != "succeeded":
                operation.status, operation.safe_error = "failed", "cleanup_turn_failed"
                self.uow.commit()
            else:
                self.uow.rollback()
            raise
