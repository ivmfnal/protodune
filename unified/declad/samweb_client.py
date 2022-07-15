from logs import Logged
import requests, json
from urllib.parse import quote


"""
import requests
import logging

# Debug logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
req_log = logging.getLogger('requests.packages.urllib3')
req_log.setLevel(logging.DEBUG)
req_log.propagate = True
"""

class SAMDeclarationError(Exception):
    def __init__(self, message, body=None):
        self.Message = message
        self.Body = body

    def __str__(self):
        out = self.Message
        if self.Body:
            out = out + "\n" + self.Body
        return out

class SAMWebClient(Logged):
    
    def __init__(self, url, cert, key):
        self.URL = url
        self.Cert = cert
        self.Key = key

    def declare(self, metadata):
        data = json.dumps(metadata, indent=1, sort_keys=True)
        response = requests.post(self.URL + "/files", data=data,
                        headers={"Content-Type":"application/json"},
			cert=(self.Cert, self.Key)
        )
        if response.status_code == 400:
            raise SAMDeclarationError("SAM declaration error", response.text)
        response.raise_for_status()
        return response.text.strip()        # file id
        
    def get_file(self, name):
        url = self.URL + "/files/name/" + quote(name) + "/metadata?format=json"
        response = requests.get(url, headers={"Accept":"application/json"})
        if response.status_code // 100 == 2:
            return response.json()
        else:
            return None

    def file_exists(self, name):
        return self.get_file(name) is not None
        
    def files_exist(self, names):
        url = self.URL + "/files/metadata"
        params = {"file_name": list(names)}
        response = requests.post(url, data=json.dumps(params))
        return set(f["file_name"] for f in response.json())

def client(config):
    if "url" in config:
        return SAMWebClient(config["url"], config.get("cert"), config.get("key"))
    else:
        return None

if __name__ == "__main__":
    import sys, os, yaml
    config = yaml.load(open(sys.argv[1], "r"), Loader=yaml.SafeLoader)["samweb"]
    c = client(config)
    meta = json.load(open(sys.argv[3], "r"))
    meta["file_name"] = filename = sys.argv[2]
    meta["user"] = config.get("user", os.getlogin())
    meta.pop("file_id", None)
    c.declare(meta)
        
