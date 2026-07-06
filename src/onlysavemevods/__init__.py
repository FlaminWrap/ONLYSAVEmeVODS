"""ONLYSAVEmeVODS application package."""

from importlib.metadata import PackageNotFoundError, version

_FALLBACK_VERSION = "0.1.0"

try:
    __version__ = version("onlysavemevods")
except PackageNotFoundError:
    __version__ = _FALLBACK_VERSION
