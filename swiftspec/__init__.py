from .core import SWIFTFileSystem
from fsspec import register_implementation

from . import _version

__version__ = _version.get_versions()["version"]

register_implementation(SWIFTFileSystem.protocol, SWIFTFileSystem)

__all__ = ["__version__", "SWIFTFileSystem"]
