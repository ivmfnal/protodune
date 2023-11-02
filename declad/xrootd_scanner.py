from pythreader import PyThread, synchronized, Primitive
from threading import Event
from tools import runCommand
import time, fnmatch, re
from logs import Logged
from file_descriptor import FileDescriptor

class ScannerError(Exception):
    pass

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
                            path = path if path.startswith(location) else location + "/" + path
                            files.append(FileDescriptor(self.Server, location, path, size))
                        elif t == "d": 
                            path = path if path.startswith(location) else location + "/" + path
                            dirs.append(path)
                        else:
                            print(f"Unknown directory entry type '{t}' in: {l} -- ignored")
        return status, error, files, dirs
        
    def getFileSize(self, file_path):
        stat_command = f"xrdfs {self.Server} stat {file_path}"
        self.debug("getFileSize: stat command:", stat_command)
        status, out = runCommand(stat_command, self.OperationTimeout, self.debug)
        #if status:
        #    raise ScannerError(f"Error in {lscommand}: {out}")
        lines = [x.strip() for x in out.split("\n")]
        for l in lines:
            l = l.strip()
            #self.debug("line:", l)
            if l:
                words = l.split()
                if len(words) == 2 and words[0] == "Size:":
                    return int(words[1])
                elif "no such file or directory" in l.lower():
                    return None
        return None
