#   Copyright 2020 CERN
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import json
import os
import urllib3
import requests

from .exceptions import *

class Request:
    def __init__(
        self,
        ucert,
        ukey,
        capath=None,
        passwd=None,
        verify=True,
        access_token=None,
        connectTimeout=30,
        timeout=30,
    ):
        self.ucert = ucert
        self.ukey = ukey
        self.passwd = passwd
        self.access_token = access_token
        self.verify = verify

        if capath:
            self.capath = capath
        elif "X509_CERT_DIR" in os.environ:
            self.capath = os.environ["X509_CERT_DIR"]
        else:
            self.capath = "/etc/grid-security/certificates"

        # Disable the warnings
        if not verify:
            urllib3.disable_warnings()

        self.connectTimeout = connectTimeout
        self.timeout = timeout

    def _handle_error(self, url, code, response_body=None):
        # Try parsing the response, maybe we can get the error message
        message = None
        response = None
        if response_body:
            try:
                response = json.loads(response_body)
                if "message" in response:
                    message = response["message"]
                else:
                    message = response_body
            except Exception:
                message = response_body

        if code == 207:
            try:
                raise ClientError("\n".join(map(lambda m: m["http_message"], response)))
            except (KeyError, TypeError):
                raise ClientError(message)
        elif code == 400:
            if message:
                raise ClientError("Bad request: " + message)
            else:
                raise ClientError("Bad request")
        elif 401 <= code <= 403:
            if message:
                raise Unauthorized(message)
            else:
                raise Unauthorized()
        elif code == 404:
            raise NotFound(url, message)
        elif code == 419:
            raise NeedDelegation("Need delegation")
        elif code == 424:
            raise FailedDependency("Failed dependency")
        elif 404 < code < 500:
            raise ClientError(str(code))
        elif code == 503:
            raise TryAgain(str(code))
        elif code >= 500:
            raise ServerError(str(code))

    def method(self, method, url, body=None, headers=None, user=None, passw=None):
        _headers = {"Accept": "application/json"}
        if headers:
            _headers.update(headers)
        if self.access_token:
            _headers["Authorization"] = "Bearer " + self.access_token

        auth = None
        if user and passw:
            from requests.auth import HTTPBasicAuth

            auth = HTTPBasicAuth(user, passw)

        if self.verify and self.capath:
            self.verify = self.capath

        response = requests.request(
            method=method,
            url=str(url),
            data=body,
            headers=_headers,
            verify=self.verify,
            timeout=(self.connectTimeout, self.timeout),
            cert=(self.ucert, self.ukey),
            auth=auth,
        )
        """
        verify: (optional) Either a boolean, in which case it controls whether we verify
            the server's TLS certificate, or a string, in which case it must be a path
            to a CA bundle to use. Defaults to ``True``.
        """

        self._handle_error(url, response.status_code, response.text)

        return str(response.text)
