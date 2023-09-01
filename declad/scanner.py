from pythreader import PyThread, synchronized, Primitive
from tools import runCommand
import time, fnmatch, traceback
from logs import Logged
from xrootd_scanner import XRootDScanner


class Scanner(PyThread, Logged):

    MetaSuffix = ".json"
    DefaultInterval = 300

    def __init__(self, receiver, config):
        PyThread.__init__(self, daemon=True, name="Scanner")
        Logged.__init__(self, f"Scanner")
        self.Receiver = receiver
        scan_config = config["scanner"]
        self.Interval = scan_config.get("interval", self.DefaultInterval)
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
            data_files = {}         # name -> desc
            metadata_files = set()  # data file names correspoinding to the metadata names
            try: files = self.XScanner.scan(self.Location)
            except:
                self.error("xrootd scanner error:", "".join(traceback.format_exc()))
            self.debug("scanner returned %d file descriptors" % (len(files,)))

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

            if not self.Stop:
                self.sleep(self.Interval)
