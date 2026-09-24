from typing import Any, Protocol

from ...normalization.schema import NormalizationResult, PreviewRequest


class NormalizerPreviewService(Protocol):
    def block_types(self) -> list[dict[str, Any]]: ...
    def preview(self, data: PreviewRequest) -> NormalizationResult: ...
