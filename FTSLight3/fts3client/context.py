#   Copyright  Members of the EMI Collaboration, 2013.
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

from datetime import datetime
import getpass
import json
import logging
import os
import sys
from urllib.parse import quote

CLIENT_VERSION="simple"

log = logging.getLogger(__name__)


#from .exceptions import *
from .request import Request, FTS3ClientException, BadEndpoint


def _get_default_proxy():
    """
    Returns the default proxy location
    """
    return "/tmp/x509up_u%d" % os.geteuid()  # nosec


class Context(object):
    def _set_x509(self, ucert, ukey):
        default_proxy_location = _get_default_proxy()

        # User certificate and key locations
        if ucert and not ukey:
            ukey = ucert
        elif not ucert:
            if "X509_USER_PROXY" in os.environ:
                ukey = ucert = os.environ["X509_USER_PROXY"]
            elif os.path.exists(default_proxy_location):
                ukey = ucert = default_proxy_location
            elif "X509_USER_CERT" in os.environ:
                ucert = os.environ["X509_USER_CERT"]
                ukey = os.environ.get("X509_USER_KEY", ucert)
            elif os.path.exists("/etc/grid-security/hostcert.pem") and os.path.exists(
                "/etc/grid-security/hostkey.pem"
            ):
                ucert = "/etc/grid-security/hostcert.pem"
                ukey = "/etc/grid-security/hostkey.pem"

        if ucert and ukey:
            self.ucert = ucert
            self.ukey = ukey
        else:
            self.ucert = self.ukey = None

        if not self.ucert and not self.ukey:
            log.warning("No user certificate given!")
        else:
            log.debug("User certificate: %s" % self.ucert)
            log.debug("User private key: %s" % self.ukey)

    def _set_endpoint(self, endpoint):
        self.endpoint = endpoint
        if self.endpoint.endswith("/"):
            self.endpoint = self.endpoint[:-1]

    def _validate_endpoint(self):
        try:
            endpoint_info = json.loads(self.get("/"))
            endpoint_info["url"] = self.endpoint
        except FTS3ClientException:
            raise
        except Exception as e:
            raise BadEndpoint("%s (%s)" % (self.endpoint, str(e))).with_traceback(
                sys.exc_info()[2]
            )
        return endpoint_info

    def _set_user_agent(self, user_agent=None):
        if user_agent is None:
            self.user_agent = "fts-python-bindings/" + CLIENT_VERSION
        else:
            self.user_agent = user_agent

    def __init__(
        self,
        endpoint,
        ucert=None,
        ukey=None,
        verify=True,
        access_token=None,
        no_creds=False,
        capath=None,
        connectTimeout=30,
        timeout=30,
        user_agent=None,
    ):
        self.passwd = None
        self.access_method = None

        self._set_user_agent(user_agent)
        self._set_endpoint(endpoint)
        if no_creds:
            self.ucert = self.ukey = self.access_token = None
        else:
            self.access_token = access_token
            if self.access_token:
                self.ucert = None
                self.ukey = None
                self.access_method = "oauth2"
            else:
                self._set_x509(ucert, ukey)
                self.access_method = "X509"

        self._requester = Request(
            self.ucert,
            self.ukey,
            passwd=self.passwd,
            verify=verify,
            access_token=self.access_token,
            capath=capath,
            connectTimeout=connectTimeout,
            timeout=timeout,
        )

        self.endpoint_info = self._validate_endpoint()
        # Log obtained information
        log.debug("Using endpoint: %s" % self.endpoint_info["url"])
        log.debug(
            "REST API version: %(major)d.%(minor)d.%(patch)d"
            % self.endpoint_info["api"]
        )

    def get_endpoint_info(self):
        return self.endpoint_info

    def get(self, path, args=None):
        if args:
            query = "&".join("%s=%s" % (k, quote(v)) for k, v in args.items())
            path += "?" + query
        return self._requester.method(
            "GET",
            "%s/%s" % (self.endpoint, path),
            headers={"User-Agent": self.user_agent},
        )

    def put(self, path, body):
        return self._requester.method(
            "PUT",
            "%s/%s" % (self.endpoint, path),
            body=body,
            headers={"User-Agent": self.user_agent},
        )

    def delete(self, path):
        return self._requester.method(
            "DELETE",
            "%s/%s" % (self.endpoint, path),
            headers={"User-Agent": self.user_agent},
        )

    def post_json(self, path, body):
        if not isinstance(body, str):
            body = json.dumps(body)
        headers = {"Content-Type": "application/json", "User-Agent": self.user_agent}
        return self._requester.method(
            "POST",
            "%s/%s" % (self.endpoint, path),
            body=body,
            headers=headers,
        )
