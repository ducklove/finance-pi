"""Finance PI: point-in-time data and backtesting for Korean equities."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("finance-pi")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["__version__"]
