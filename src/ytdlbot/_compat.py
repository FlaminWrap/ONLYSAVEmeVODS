from __future__ import annotations

from importlib import import_module
import sys


def alias_module(old_name: str) -> None:
    new_name = old_name.replace("ytdlbot", "onlysavemevods", 1)
    module = import_module(new_name)
    sys.modules[old_name] = module
