"""ONLYSAVEmeVODS application package."""

from importlib.metadata import PackageNotFoundError, version

# Source checkouts are development builds. Release tarballs rewrite this and
# pyproject.toml to the published tag version.
_FALLBACK_VERSION = "0.1.1.dev0"

try:
    __version__ = version("onlysavemevods")
except PackageNotFoundError:
    __version__ = _FALLBACK_VERSION
