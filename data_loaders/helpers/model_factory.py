"""Model construction helpers for performance/validation trade-offs.

This module allows loaders to construct immutable models quickly by default,
while preserving an opt-in full-validation path for debugging and CI.
"""

from __future__ import annotations

import os
from typing import Any, TypeVar

T = TypeVar("T")


def should_validate_models() -> bool:
    """Return True when loader model validation is explicitly enabled."""
    raw = os.getenv("H2RES_VALIDATE_MODELS", "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def build_model(model_cls: type[T], /, **data: Any) -> T:
    """Construct either Pydantic or dataclass-like models.

    Behavior:
    - Validation enabled (`H2RES_VALIDATE_MODELS=1`): use Pydantic validation
      when available, otherwise call the class constructor.
    - Validation disabled (default): use fast Pydantic `model_construct` when
      available, otherwise call the class constructor.
    """
    if should_validate_models():
        if hasattr(model_cls, "model_validate"):
            return model_cls.model_validate(data)  # type: ignore[attr-defined]
        return model_cls(**data)  # type: ignore[misc]

    if hasattr(model_cls, "model_construct"):
        return model_cls.model_construct(**data)  # type: ignore[attr-defined]
    return model_cls(**data)  # type: ignore[misc]
