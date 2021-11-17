import argparse
import logging
import os
from typing import Dict

import requests

from hdfs_utils import get_webhdfs_request_root_url, get_default_webhdfs_request_parameters, init_logging

if __name__ == "__main__":

    _LOGGING_LEVEL = "logging_level"
    _LOCAL_FILE = "local_file"
    _HDFS_PATH = "hdfs_path"

    parser = argparse.ArgumentParser()
    parser.add_argument('-local', "--local",
                        help="Local file to put on HDFS",
                        type=str,
                        dest=_LOCAL_FILE,
                        required=True,
                        metavar="")

    parser.add_argument('-hdfs', "--hdfs",
                        help="Remote HDFS path where data will be put",
                        type=str,
                        dest=_HDFS_PATH,
                        required=True,
                        metavar="")

    parser.add_argument('-log', "--log",
                        help=f"logging level ({', '.join(['DEBUG', 'INFO', 'WARN|WARNING', 'ERROR', 'FATAL|CRITICAL'])}). Case insensitive",
                        type=str,
                        dest=_LOGGING_LEVEL,
                        required=False,
                        default=logging.INFO,
                        metavar="")

    provided_args = parser.parse_args()
    init_logging(getattr(provided_args, _LOGGING_LEVEL))
    log = logging.getLogger(__name__)

    local_file_path = getattr(provided_args, _LOCAL_FILE)
    hdfs_path = getattr(provided_args, _HDFS_PATH)

    log.debug(f"Local file path: {local_file_path}")
    log.debug(f"HDFS path: {hdfs_path}")
    file_name = os.path.basename(local_file_path)
    log.debug(f"File name: {file_name}")

    # PUT "http://nn.example.com:50070/webhdfs/v1/tmp/testfile?op=CREATE&overwrite=true"
    first_request_extra_params: Dict[str, str] = {
        "op": "CREATE",
        "overwrite": "true",
        "recursive": "true"
    }
    first_request_params: Dict[str, str] = {** get_default_webhdfs_request_parameters(), ** first_request_extra_params}
    first_put_request = requests.put(f"{get_webhdfs_request_root_url()}/{hdfs_path}/{file_name}")

    PATH = "/user/osboxes/apps/aurora_dataload/"
    get_request_params: Dict[str, str] = get_default_webhdfs_request_parameters()
    get_request_params["op"] = "LISTSTATUS"
    list_status_request = requests.get(f"{get_webhdfs_request_root_url()}/{PATH}", params=get_request_params)
    print(list_status_request.json())
