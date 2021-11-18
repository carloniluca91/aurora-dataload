import argparse
import json
import logging
import os
import sys
from typing import Dict, List

from requests import Response

from hdfs_request import FSRequest, RequestType
from utils import init_logging


class FSPutRequest(FSRequest):

    def __init__(self,
                 local_file_path: str,
                 hdfs_path: str):

        self._log = logging.getLogger(__name__)
        file_name = os.path.basename(local_file_path)
        hdfs_path_normalized: str = FSPutRequest.normalize_path(hdfs_path)
        hdfs_put_request_path: str = f"{hdfs_path_normalized}/{file_name}"
        self._log.debug(f"Local file path: {local_file_path}, HDFS put request path: {hdfs_put_request_path}")
        additional_parameters: Dict[str, str] = {
            "op": "CREATE",
            "overwrite": "true",
            "recursive": "true"
        }

        super().__init__(RequestType.PUT,
                         request_path=hdfs_put_request_path,
                         additional_headers=None,
                         additional_parameters=additional_parameters,
                         request_data=open(local_file_path))

    def _format_successful_response(self, response: Response):
        return None

    def _format_failed_response(self, response: Response):

        response_text_dict = json.loads(response.text)
        remote_exception: Dict[str, str] = response_text_dict["RemoteException"]
        exception: str = remote_exception["exception"]
        java_class_name: str = remote_exception["javaClassName"]
        stack_trace: List[str] = remote_exception["message"].split("\n\t")
        pretty_dict = {
            "exception": exception,
            "javaClassName": java_class_name,
            "stackTrace": stack_trace
        }

        return json.dumps(pretty_dict, indent=4)

    @staticmethod
    def normalize_path(path: str) -> str:

        cut_beginning_slash = path if not path.startswith("/") else path[1:]
        return cut_beginning_slash if not cut_beginning_slash.endswith("/") else cut_beginning_slash[:-1]


if __name__ == "__main__":

    _LOGGING_LEVEL = "logging_level"
    parser = argparse.ArgumentParser()
    parser.add_argument('-log', "--log",
                        help=f"logging level ({', '.join(['DEBUG', 'INFO', 'WARN|WARNING', 'ERROR', 'FATAL|CRITICAL'])}). Case insensitive",
                        type=str,
                        dest=_LOGGING_LEVEL,
                        required=False,
                        default="INFO",
                        metavar="")

    (namespace, unknown_args) = parser.parse_known_args()
    init_logging(getattr(namespace, _LOGGING_LEVEL))
    FSPutRequest(sys.argv[1], sys.argv[2]).run_request()
