import requests, json, time
from .context import Context
from .exceptions import FTS3ClientException

class FTS3TransferTimeout(FTS3ClientException):
    pass

class FTS3TransferError(FTS3ClientException):

    def __init__(self, message):
        self.Message = message
        
    def __str__(self):
        return f"FTS3TransferError: {self.Message}"

class FTS3Transfer(object):

    def __init__(self, client, job_id, src, dst):
        self.Src = src
        self.Dst = dst
        self.JobID = job_id
        self.Client = client
        self.Failed = False
        self.Error = None
        self.State = None
        self.Adler32 = None

    def state(self):
        status = self.update_status()
        return self.State, self.Reason

    def update_status(self):
        #print("updating status...")
        data = self.Client.job_status(self.JobID)
        files = data["files"] = self.Client.files(self.JobID)
        #print("status updated")
        self.State = data.get("job_state")
        self.Reason = data.get("reason")
        assert isinstance(files, list) and len(files) == 1
        self.Checksums = {}
        checksums = files[0].get("checksum", "").split()
        self.Adler32 = None
        for v in checksums:
            try:
                name, value = v.split(":", 1)
                if name.lower() == "adler32":
                    self.Adler32 = value
                    break
            except:
                pass
        return data
        
    def wait(self, timeout=None, poll_interval=10):
        # on success: return True, self.Failed == False
        # on error: return True, self.Failed = True, self.Erorr contains the error
        # on timeout returns False
        done = False
        t0 = time.time()
        t1 = None if timeout is None else t0 + timeout
        while not done and (t1 is None or time.time() < t1):
            self.update_status()
            state = self.State
            #print("FTS3.wait: state:", state)
            if state == "FAILED":
                self.Failed = True
                done = True
            elif state in ("STAGING", "SUBMITTED", "ACTIVE"):
                dt = poll_interval
                if t1 is not None:
                    dt = max(0.0, min(dt, t1 - time.time()))
                    if dt <= 0:
                        break       # timeout
                #print("   sleep dt:", dt, "t1:", t1, "now:", time.time())
                time.sleep(dt)
            elif state == "FINISHED":
                done = True
            else:
                raise FTS3TransferError(f"Unknown transfer state: {state}  reason: {reason}")
        return done

class FTS3(object):

    def __init__(self, url_head, proxy_file):
        self.URLHead = url_head
        self.ProxyFile = proxy_file
        self.Context = Context(url_head, self.ProxyFile, None, verify=False)

    def submit(self, src_url, dst_url, metadata = None, overwrite=True, **meta_args):
        params = {"overwrite":overwrite}
        request = {
            "params": params,
            "files": [
                {
                    "sources": [src_url],
                    "destinations": [dst_url]
                }
            ]    
        }
        if metadata or meta_args:
            metadata = (metadata or {}).copy()
            metadata.update(meta_args)
            request["metadata"] = metadata
        response = self.Context.post_json("/jobs", request)
        #print(response)
        job_id = json.loads(response)["job_id"]
        return FTS3Transfer(self, job_id, src_url, dst_url)

    def job_status(self, job_id):
        status_response = self.Context.get("/jobs/"+job_id)
        return json.loads(status_response)

    def files(self, job_id):
        status_response = self.Context.get(f"/jobs/{job_id}/files")
        return json.loads(status_response)

if __name__ == "__main__":
    import sys

    Usage = """Usage:
    python fts3client.py <server> <proxy> <src url> <dst url>
    """

    if len(sys.argv[1:]) != 4:
        print(Usage)
        sys.exit(2)

    server, proxy, src, dst = sys.argv[1:]

    fts3 = FTS3(server, proxy)
    transfer = fts3.submit(src, dst)
    print("Job ID:", transfer.JobID)
    print(transfer.wait())
    print(transfer.State, transfer.Error)


