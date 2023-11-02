from pythreader import PyThread
from tools import runCommand
import time, fnmatch, traceback, re, stat, os.path
from logs import Logged
from file_descriptor import FileDescriptor

class LocalScanner(PyThread, Logged):

    DefaultMetaSuffix = ".json"
    DefaultInterval = 300

    # Linux ls -l pattern
    #
    # Linux ls -l output line:
    # -rw-r--r-- 1 ivm3 ivm3 1228 Mar 22 16:52 /home/ivm3/token
    DefaultParseRE = r"(?P<type>[a-z-])\S+\s+\d+\s+\S+\s+\S+\s+(?P<size>\d+)\s+\S+\s+\d+\s+\S+\s+(?P<path>\S+)$"

    def __init__(self, receiver, config):
        PyThread.__init__(self, daemon=True, name="Scanner")
        Logged.__init__(self, f"Scanner")
        self.Receiver = receiver
        scan_config = config["scanner"]
        self.Interval = scan_config.get("interval", self.DefaultInterval)
        self.Location = scan_config["location"]
        self.ReplaceLocation = scan_config.get("replace_location")
        self.lsCommandTemplate = scan_config["ls_command_template"]            
        self.ParseRE = re.compile(scan_config.get("parse_re", self.DefaultParseRE))
        patterns = scan_config.get("filename_patterns") or scan_config.get("filename_pattern")
        if not patterns:
            raise ValueError("Filename patterns (filename_patterns) not found in the config file")
        self.FilenamePatterns = patterns if isinstance(patterns, list) else [patterns]
        self.MetaSuffix = config.get("meta_suffix", self.DefaultMetaSuffix)
        self.MetadataPatterns = [pattern + self.MetaSuffix for pattern in self.FilenamePatterns]
        self.Stop = False
        self.Server = None
        
    def do_ls(self, location, timeout=None):
        lscommand = self.lsCommandTemplate.replace("$location", location)
        #print("lscommand:", lscommand)
        files = []
        dirs = []
        error = ""
        status, out = runCommand(lscommand, timeout, self.debug)
        if status:
            error = out
            self.log("ls (%s) error: %s" % (lscommand, error,))
        else:
            lines = [x.strip() for x in out.split("\n")]
            for l in lines:
                if l:
                    m = self.ParseRE.match(l)
                    if m:
                        t = m["type"]
                        path = m["path"]
                        orig_path = path = path if path.startswith(location) else location + "/" + path
                        if self.ReplaceLocation:
                            path = self.ReplaceLocation + path[len(location):]
                        if t in "f-":
                            size = int(m["size"])
                            if size == 0:
                                self.debug("Zero file size in:\n   ", l)
                            files.append(FileDescriptor(self.Server, location, path, size))
                        elif t == "d": 
                            dirs.append(path)
                        else:
                            self.log(f"Unknown directory entry type '{t}' in: {l} -- ignored")
        return status, error, files, dirs

    def ls_files(self):
        status, error, files, _ = self.do_ls(self.Location)
        if status:
            return None, error
        return [
            desc for desc in files
            if any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.FilenamePatterns + self.MetadataPatterns) 
        ], None

    def run(self):
        while not self.Stop:
            if self.Receiver.low_water():
                data_files = {}         # name -> desc
                metadata_files = set()  # data file names correspoinding to the metadata names
                try: 
                    files, error = self.ls_files()
                except:
                    self.log("ls traceback:", "".join(traceback.format_exc()))
                if error:
                    self.log("ls error:", error)
                else:
                    self.debug("scanner returned %d file descriptors" % (len(files,)))
                    #for f in files:
                    #    print(f)

                    out_files = {}

                    for desc in files:
                        if any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.FilenamePatterns):
                            meta_name = desc.Name + self.MetaSuffix
                            if desc.Name in metadata_files:
                                out_files[desc.Name] = desc
                            else:
                                data_files[desc.Name] = desc
                        elif any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.MetadataPatterns) and desc.Size > 0:
                            data_name = desc.Name[:-len(self.MetaSuffix)]
                            data_desc = data_files.get(data_name)
                            if data_desc is not None:
                                out_files[data_name] = data_desc
                            else:
                                metadata_files.add(data_name)

                    self.log("found %d matching files" % (len(out_files),))
            
                    if out_files:
                        #self.debug("sending files:")
                        #for fn, desc in out_files.items():
                        #    self.debug(desc.Path, fn)
                        self.Receiver.add_files(out_files)
            else:
                self.log("scan is not needed as the receiver is above the low water mark")

            if not self.Stop:
                self.sleep(self.Interval)
