import logging
from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Callable, Union, Any
from typing import Dict

import requests
from requests import Response

_HDFS_HOST = "quickstart-bigdata"
_WEBHDFS_PORT = 9870
_USER_NAME = "osboxes"


@unique
class RequestType(Enum):

    GET = 1
    POST = 2
    PUT = 3


class FSRequest(ABC):

    _REQUEST_TYPE_DICT: Dict[RequestType, Callable] = {
        RequestType.GET: requests.get,
        RequestType.POST: requests.post,
        RequestType.PUT: requests.put
    }

    def __init__(self,
                 request_type: RequestType,
                 request_path: str,
                 additional_headers: Union[Dict[str, str], None],
                 additional_parameters: Union[Dict[str, str], None],
                 request_data: Union[Any, None]):

        self._log = logging.getLogger(__name__)
        self._request_type: RequestType = request_type
        self._request_path: str = request_path
        self._additional_headers: Dict[str, str] = additional_headers
        self._additional_parameters: Dict[str, str] = additional_parameters
        self._request_data = request_data

    def run_request(self):

        request_url: str = f"{FSRequest.get_webhdfs_request_root_url()}/{self._request_path}"
        request_headers: Dict[str, str] = FSRequest.merge_dicts(FSRequest.get_basic_request_headers(), self._additional_headers)
        request_parameters: Dict[str, str] = FSRequest.merge_dicts(FSRequest.get_basic_request_parameters(), self._additional_parameters)
        response: Response = FSRequest._REQUEST_TYPE_DICT[self._request_type](
            request_url,
            headers=request_headers,
            params=request_parameters,
            data=self._request_data)

        if response:
            response_pretty_text: str = self._format_successful_response(response)
            response_final_pretty_text: str = f". Response text: {response_pretty_text}" if response_pretty_text is not None else ""
            self._log.info(f"Successfully executed request{response_final_pretty_text}")
        else:
            response_pretty_text: str = self._format_failed_response(response)
            self._log.error(f"Failed to execute request. Response text: {response_pretty_text}")

    @abstractmethod
    def _format_successful_response(self, response: Response) -> str:
        pass

    @abstractmethod
    def _format_failed_response(self, response: Response) -> str:
        pass

    @staticmethod
    def get_webhdfs_request_root_url() -> str:
        return f"http://{_HDFS_HOST}:{_WEBHDFS_PORT}/webhdfs/v1"

    @staticmethod
    def get_basic_request_parameters() -> Dict[str, str]:
        return {
            "user.name": _USER_NAME
        }

    @staticmethod
    def get_basic_request_headers() -> Dict[str, str]:
        return {
            "content-Type": "application/json",
            "Accept-Charset": "UTF-8"
        }

    @staticmethod
    def merge_dicts(first: Dict[str, str], second: Dict[str, str]):

        return first if second is None else {
            ** first,
            ** second
        }
