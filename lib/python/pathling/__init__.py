"""
Important classes of Pathling Python API:
    - :class:`pathling.PathlingContext`
      Main entry point for Pathling API functionality.
"""
from ._version import __version__
from .context import PathlingContext

__all__ = [
    "PathlingContext",
]
