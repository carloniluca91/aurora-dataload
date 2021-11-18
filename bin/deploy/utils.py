import logging
import sys

from typing import Dict, Union


def init_logging(logging_level: Union[int, str]) -> None:

    logging_level_dict: Dict[str, int] = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARN,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "FATAL": logging.FATAL,
        "CRITICAL": logging.CRITICAL
    }

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s <%(lineno)d>: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        level=logging_level if isinstance(logging_level, int) else logging_level_dict[logging_level.upper()])
