from pythreader import PyThread, synchronized, Primitive
from tools import runCommand
import time, fnmatch, traceback
from logs import Logged
from xrootd_scanner import XRootDScanner


class Scanner(PyThread, Logged):

    MetaSuffix = ".json"
    DefaultInterval = 300

    def __init__(self, receiver, config, interval=None):
        PyThread.__init__(self, daemon=True, name="Scanner")
        Logged.__init__(self, f"Scanner")
        self.Interval = interval or self.DefaultInterval
        self.Receiver = receiver
        scan_config = config["scanner"]
        self.Recursive = scan_config.get("recursive", False)
        self.Server, self.Location = scan_config["server"], scan_config["location"]
        self.XScanner = XRootDScanner(self.Server, scan_config)
        patterns = scan_config.get("filename_patterns") or scan_config.get("filename_pattern")
        if not patterns:
            raise ValueError("Filename patterns (filename_patterns) not found in the config file")
        self.FilenamePatterns = patterns if isinstance(patterns, list) else [patterns]
        self.MetaSuffix = config.get("meta_suffix", ".json")
        self.MetadataPatterns = [pattern + self.MetaSuffix for pattern in self.FilenamePatterns]
        self.Stop = False

    def ls_input(self):
            try: files = self.XScanner.scan(self.Location)
            except:
                return None, "xrootd scanner error: " + "".join(traceback.format_exc())

            return [
                desc for desc in files
                if any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.FilenamePatterns + self.MetadataPatterns) 
            ], None

        
    def run(self):
        while not self.Stop:
            try: files = self.XScanner.scan(self.Location, self.Recursive)
            except:
                self.error("xrootd scanner error:", "".join(traceback.format_exc()))
            self.debug("scanner returned %d file descriptors" % (len(files,)))

            metadata_files = set()  # data file paths correspoinding to the metadata paths
            data_files = {}     # path -> desc
            paired_files = {}      # path -> FileDescriptor

            for desc in files:
                if any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.FilenamePatterns):
                    meta_path = desc.Path + self.MetaSuffix
                    if meta_path in metadata_files:
                        paired_files[desc.Path] = desc
                    else:
                        data_files[desc.Path] = desc
                elif any(fnmatch.fnmatch(desc.Name, pattern) for pattern in self.MetadataPatterns) and desc.Size > 0:
                    data_path = desc.Path[:-len(self.MetaSuffix)]
                    data_desc = data_files.get(data_path)
                    if data_desc is not None:
                        paired_files[data_path] = data_desc
                    else:
                        metadata_files.add(data_path)

            self.log("found %d matching files" % (len(paired_files),))
            
            if paired_files:
                #self.debug("sending files:")
                #for fn, desc in out_files.items():
                #    self.debug(desc.Path, fn)
                self.Receiver.add_files(paired_files.values())

            if not self.Stop:
                self.sleep(self.Interval)
