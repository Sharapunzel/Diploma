from typing import Any

from ...core.errors import DomainError
from ...normalization.catalog import BLOCK_TYPES
from ...normalization.compiler import RuleValidationError
from ...normalization.engine import NormalizationEngine
from ...normalization.schema import NormalizationResult, PreviewRequest


class NormalizerPreviewServiceImpl:
    def __init__(self, engine: NormalizationEngine):
        self.engine = engine

    def block_types(self) -> list[dict[str, Any]]:
        return BLOCK_TYPES

    def preview(self, data: PreviewRequest) -> NormalizationResult:
        rule = data.rule.model_dump(mode="json", exclude_none=True)
        try:
            self.engine.compile(rule)
        except RuleValidationError as error:
            raise DomainError(
                "normalizer_rule_invalid", str(error), 422, error.details,
            ) from None
        return self.engine.normalize(rule, data.sample)
