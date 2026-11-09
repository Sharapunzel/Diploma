"""Worker authority and safe, stable diagnostic vocabulary."""
from dataclasses import dataclass
from uuid import UUID

from .contracts import DetectionError

# Two-int advisory namespace; distinct from WAL/bootstrap locks.
LOCK_CLASS = 1380205362
LOCK_ID = 1


@dataclass(frozen=True)
class ExecutionOwner:
    owner_id: UUID
    generation: int
    backend_pid: int


REASONS = {
    "managed_worker_required": "Вычисления выполняет worker. Используйте start и state.",
    "warmup_required": "Требуется явный запуск и подготовка модели.",
    "working_point_pending": "Ожидание следующей готовой точки агрегата.",
    "working_point_gap": "Ожидание отсутствующей точки; неизвестный интервал не пропускается.",
    "working_point_not_completed": "Окно ещё не завершено.",
    "aggregate_inactive": "Агрегат недоступен или архивирован.",
    "aggregate_deleted": "Агрегат удалён; дальнейший запуск запрещён.",
    "start_interrupted": "Первоначальный запуск прерван. Требуется новый ручной запуск.",
    "start_failed": "Запуск не завершён. Требуется новый ручной запуск.",
    "execution_failed": "Ошибка исполнения. Требуется новый ручной запуск.",
    "temporary_unavailable": "Временная недоступность хранилища. Состояние требует повторного чтения.",
    "manual_stop": "Детектор остановлен вручную; модель и курсор сохранены.",
    "checkpoint_invalid": "Сохранённая модель повреждена или несовместима; новая модель не создаётся.",
    "history_insufficient": "Недостаточно пригодной истории для прогрева. Повторите запуск вручную после накопления данных.",
    "history_quality": "История содержит непригодные значения; проверьте качество признаков.",
    "resource_limit": "Превышен допустимый ресурсный бюджет.",
    "ownership_lost": "Координатор потерял владение; результат вычисления отменён.",
}


def safe_reason(reason):
    value = str(reason)
    if value in REASONS:
        return value
    if value == "source_unavailable":
        return "aggregate_inactive"
    if value.startswith(("checkpoint_", "runtime_")):
        return "checkpoint_invalid"
    if value.startswith("resource_budget"):
        return "resource_limit"
    if value.startswith(("insufficient", "history_before", "warmup_history", "historical_time_basis", "legacy_coverage")):
        return "history_insufficient"
    if value.startswith(("coverage:", "feature_", "float32_", "constant_history", "warmup_")):
        return "history_quality"
    return "execution_failed"


def require_owner(owner):
    if not isinstance(owner, ExecutionOwner):
        raise DetectionError("managed_worker_required")


def reason_details(reason):
    """Only declared factors cross the diagnostic boundary, never raw errors."""
    prefix, _, factor = str(reason).partition(":")
    if prefix == "resource_budget" and factor in {
        "model_seconds", "model_memory", "checkpoint_bytes", "warmup_input_bytes",
        "warmup_windows", "preparation_seconds",
        "preview_rows", "statement_width", "operator_cost", "total_memory", "array_elements",
    }:
        return {"factor": factor}
    return {}
