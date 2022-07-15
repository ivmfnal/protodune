from pythreader import synchronized, Primitive, TaskQueue, Task
import time, fnmatch, traceback
from logs import Logged
from xrootd_scanner import XRootDScanner

class ScannerTask(Task, Logged):
    
    DefaultInterval = 60
    
    def __init__(self, config, server, location):
        Logged.__init__(self, name=f"ScannerTask({server}:{location})")
        Task.__init__(self)
        self.Server = server
        self.Location = location
        self.Config = config
        self.MetaSuffix = config.get("meta_suffix", ".json")
        self.ResubmitInterval = config.get("interval", self.DefaultInterval)            # for TaskQueue
        self.Resubmit = True
        
        patterns = config.get("filename_patterns") or config.get("filename_pattern")
        if not patterns:
            raise ValueError("Filename patterns (filename_patterns) not found in the config file")
        self.FilenamePatterns = patterns if isinstance(patterns, list) else [patterns]
        
    def run(self):
        xscanner = XRootDScanner(self.Server, self.Config)
        data_files = {}         # name -> desc
        metadata_files = set()  # data file names correspoinding to the metadata names
        files = xscanner.scan(self.Location)

        meta_suffix_len = len(self.MetaSuffix)

        meta_names = {
            desc.Name[:meta_suffix_len] for desc in files
            if desc.Name.endswith(self.MetaSuffix)
        }

        self.log("found files (data and metadata):", len(files), "    metadata:", len(meta_names))

        out = [desc for desc in files
            if desc.Name in meta_names
                and any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.FilenamePatterns)
        ]

        self.log("found %d matching data/metadata pairs" % (len(out),))
        return out

class Scanner(Primitive, Logged):


    def __init__(self, receiver, scan_config):
        PyThread.__init__(self, daemon=True, name="Scanner")
        Logged.__init__(self, f"Scanner")
        self.Config = scan_config
        self.MaxScanners = scan_config.get("max_scanners")
        self.Receiver = receiver
        self.Servers = scan_config["servers"]           # [ {"host":"...", locations:[...]}]
        self.Stop = False
        self.ScannerQueue = TaskQueue(self.MaxScanners, delegate=self, stagger=1.0)

    def ls(self, server, location):
        xscanner = XRootDScanner(server, self.Config)
        try: files = xscanner.scan(location)
        except:
            return None, "xrootd scanner error: " + "".join(traceback.format_exc())

        return [
            desc for desc in files
            if any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.FilenamePatterns + self.MetadataPatterns) 
        ], None

    def start(self):
        for server_info in self.Servers:
            host = server_info["host"]
            locations = server_info["locations"]
            for location in locations:
                task = ScannerTask(self.Config, server, location)
                self.ScannerQueue.add(task)

    def taskEnded(self, queue, task, files):
        task.Resubmit = not self.Stop
        if files:
            self.Receiver.add_files(files)

    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        task.Resubmit = not self.Stop
        self.error("Scanner task exception:", '\n'.join(traceback.format_exception(exc_type, exc_value, tb)))

    def join(self):
        self.ScannerQueue.waitUntilEmpty()
        
