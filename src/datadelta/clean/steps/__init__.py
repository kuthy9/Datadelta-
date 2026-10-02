"""
steps — The registry of clean steps.

Each step lives in its own module (headers.py, numbers.py, ...) exposing
NAME and apply(df, ctx). Registering a step means importing its module
here and listing it in _STEP_MODULES; clean_frame() runs the registered
steps in STEP_ORDER, so the order of _STEP_MODULES does not matter for
execution (it is kept in STEP_ORDER order for readability).
"""

from __future__ import annotations

from types import ModuleType

from .base import StepFn
from .     import booleans, case, dates, dedupe, headers, impute, null_tokens, numbers, whitespace


# ── Registered step modules ───────────────────────────────────────────────────

_STEP_MODULES: tuple[ModuleType, ...] = (
    headers, whitespace, null_tokens, booleans, numbers, dates, case, dedupe, impute,
)

STEP_FUNCS: dict[str, StepFn] = {module.NAME: module.apply for module in _STEP_MODULES}
