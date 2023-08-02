from pythreader import PyThread, synchronized, Primitive
from threading import Event
from tools import runCommand
import time, fnmatch, re
from logs import Logged

class ScannerError(Exception):
    pass

class FileDescriptor(object):

    def __init__(self, server, location, path, name, size):
        self.Server = server
        self.Location = location
        self.Path = path
        self.Name = name
        self.Size = size

        assert path.startswith(location)
        relpath = path[len(location):]
        while relpath and relpath[0] == "/":
            relpath = relpath[1:]
        self.Relpath = relpath              # path relative to the location root, with leading slash removed
       
    def __str__(self):
        return "%s:%s:%s" % (self.Server, self.Path, self.Size)
        
    __repr__ = __str__

class XRootDScanner(Logged):

    # generic xrootd server
    DefaultParseRE = r"^(?P<type>[a-z-])\S+\s+\S+\s+\S+\s+(?P<size>\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(?P<path>\S+)$"

    def __init__(self, server, config):
        Logged.__init__(self, f"XRootDScanner")
        self.Recursive = False
        self.Server = server
        self.lsCommandTemplate = config["ls_command_template"].replace("$server", self.Server)                
        self.ParseRE = re.compile(config["parse_re"])
        self.OperationTimeout = config.get("timeout", 30)
                
    def scan(self, location):
        status, error, file_descs, _ = self.listFilesAndDirs(location, self.OperationTimeout)
        if status == 0:
            return file_descs
        else:
            raise RuntimeError("Error scanning %s: status=%s error=%s" % (location, status, error))

    def listFilesAndDirs(self, location, timeout):
        lscommand = self.lsCommandTemplate.replace("$location", location)
        files = []
        dirs = []
        error = ""
        status, out = runCommand(lscommand, timeout, self.debug)
        if status:
            error = out
            self.log("Error in ls (%s): %s" % (lscommand, error,))
        else:
            lines = [x.strip() for x in out.split("\n")]
            for l in lines:
                l = l.strip()
                if l:
                    m = self.ParseRE.match(l)
                    if m:
                        t = m["type"]
                        path = m["path"]
                        if t in "f-":
                            size = int(m["size"])
                            if size == 0:
                                self.debug("Zero file size in:\n   ", l)
                            name = path.rsplit("/",1)[-1]
                            path = path if path.startswith(location) else location + "/" + path
                            files.append(FileDescriptor(self.Server, location, path, name, size))
                        elif t == "d": 
                            path = path if path.startswith(location) else location + "/" + path
                            dirs.append(path)
                        else:
                            print(f"Unknown directory entry type '{t}' in: {l} -- ignored")
        return status, error, files, dirs
        
    def getFileSize(self, file_path):
        lscommand = self.lsCommandTemplate.replace("$location", file_path)
        status, out = runCommand(lscommand, self.OperationTimeout, self.debug)
        #if status:
        #    raise ScannerError(f"Error in {lscommand}: {out}")
        lines = [x.strip() for x in out.split("\n")]
        for l in lines:
            l = l.strip()
            #self.debug("line:", l)
            if l:
                m = self.ParseRE.match(l)
                if m:
                    t = m["type"]
                    path = m["path"]
                    if t in "f-" and path == file_path:
                        size = int(m["size"])
                        if size == 0:
                            self.debug("Zero size in line:", l)
                        return size
                    #else:
                    #    raise ScannerError(f"Unknown directory entry type '{t}' in: {l}")
                elif "no such file or directory" in l.lower():
                    break
        return None
