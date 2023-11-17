"""Documentation about fs_irods."""
import logging
from .iRODSFS import iRODSFS

logging.getLogger(__name__).addHandler(logging.NullHandler())

__author__ = "Helge Hecht"
__email__ = "helge.hecht@recetox.muni.cz"
__version__ = "0.1.0"

__all__ = ["iRODSFS"]