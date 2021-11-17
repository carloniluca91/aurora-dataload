import logging
import sys

from typing import Dict

_HDFS_HOST = "quickstart-bigdata"
_WEBHDFS_PORT = 9870
_USER_NAME = "osboxes"


def get_webhdfs_request_root_url() -> str:
    return f"http://{_HDFS_HOST}:{_WEBHDFS_PORT}/webhdfs/v1"


def get_default_webhdfs_request_parameters() -> Dict[str, str]:
    return {
        "user.name": _USER_NAME
    }


def init_logging(logging_level: str) -> None:

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
        level=logging_level_dict[logging_level.upper()])
