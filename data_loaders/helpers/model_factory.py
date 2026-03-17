"""Model construction helpers for performance/validation trade-offs.

This module centralizes validation-mode handling so loaders can build models
quickly by default while preserving opt-in strict validation for debugging
and CI.
"""

from __future__ import annotations

import os
from typing import Any, Literal, TypeVar

T = TypeVar("T")
ValidationMode = Literal["fast", "strict", "off"]

def validation_mode() -> ValidationMode:
    """Return the active loader validation mode.

    Precedence:
    1. ``H2RES_VALIDATION_MODE=fast|strict|off``
    2. default ``fast``
    """
    raw_mode = os.getenv("H2RES_VALIDATION_MODE", "").strip().lower()
    if raw_mode in {"fast", "strict", "off"}:
        return raw_mode  # type: ignore[return-value]

    return "fast"


def should_validate_models() -> bool:
    """Return True when full model validation is explicitly enabled."""
    return validation_mode() == "strict"


def is_strict_validation() -> bool:
    return validation_mode() == "strict"


def is_fast_validation() -> bool:
    return validation_mode() == "fast"


def is_validation_off() -> bool:
    return validation_mode() == "off"


def build_model(model_cls: type[T], /, **data: Any) -> T:
    """Construct either Pydantic or dataclass-like models.

    Behavior:
    - ``strict``: use Pydantic validation when available.
    - ``fast`` / ``off``: use fast Pydantic ``model_construct`` when available.
    """
    if is_strict_validation():
        if hasattr(model_cls, "model_validate"):
            return model_cls.model_validate(data)  # type: ignore[attr-defined]
        return model_cls(**data)  # type: ignore[misc]

    if hasattr(model_cls, "model_construct"):
        return model_cls.model_construct(**data)  # type: ignore[attr-defined]
    return model_cls(**data)  # type: ignore[misc]
