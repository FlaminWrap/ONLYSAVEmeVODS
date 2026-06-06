"""Compatibility package for the renamed ONLYSAVEmeVODS project."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_target = import_module("onlysavemevods")
__version__ = getattr(_target, "__version__", "0.0.0")


def __getattr__(name: str) -> Any:
    return getattr(_target, name)
