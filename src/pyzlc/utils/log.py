import logging
from enum import IntEnum

from colorama import Fore, init

init(autoreset=True)


class LogLevel(IntEnum):
    """Log levels for pyzlc logging."""

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class CustomFormatter(logging.Formatter):
    """Custom log formatter that adds colors"""

    FORMAT = "[%(asctime)s] [%(levelname)s] %(message)s"

    FORMATS = {
        logging.DEBUG: Fore.YELLOW + FORMAT + Fore.RESET,
        logging.INFO: Fore.BLUE + FORMAT + Fore.RESET,
        logging.WARNING: Fore.RED + FORMAT + Fore.RESET,
        logging.ERROR: Fore.MAGENTA + FORMAT + Fore.RESET,
        logging.CRITICAL: Fore.CYAN + FORMAT + Fore.RESET,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.FORMAT)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_logger():
    """Create and return a custom logger"""
    logger_ = logging.Logger("LanComLogger")
    logger_.setLevel(logging.DEBUG)

    # Create console handler and set level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create custom formatter and add to handler
    formatter = CustomFormatter()
    ch.setFormatter(formatter)

    # Add handler to logger
    if not logger_.handlers:
        logger_.addHandler(ch)

    return logger_


# Get the logger
_logger = get_logger()
