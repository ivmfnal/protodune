from pythreader import ShellCommand

def to_bytes(s):    
    return s if isinstance(s, bytes) else s.encode("utf-8")

def to_str(b):    
    return b if isinstance(b, str) else b.decode("utf-8", "ignore")

def runCommand(cmd, timeout=None, debug=None):
    if timeout is not None and timeout < 0: timeout = None
    if debug:
        debug("runCommand: %s" % (cmd,))
    status, out, err = ShellCommand.execute(cmd, timeout=timeout)
    if debug:
        debug("%s [%s] [%s]" % (status, out, err))
        
    if not out: out = err
    
    if status is None:
        cmd.kill()
        out = (out or "") + "\n subprocess timed out\n"
        status = 100
    
    return status, out
    
class MemoryLog(Primitive):

    def __init__(self, capacity=100):
        Primitive.__init__(self)
        self.Log = []       # (timestamp, message)
        self.Capacity = capacity

    @synchronized
    def log(self, *what):
        t = time.time()
        msg = " ".join(str(x) for x in what)
        self.Log.append((t, msg))
        self.Log = self.Log[-self.Capacity:]
        
    def getLog(self):
        return self.Log[:]

class FileDescriptor(object):

    def __init__(self, server, location, path, name, size):
        self.Server = server
        self.Location = location
        self.Path = path
        self.Name = name
        self.Size = size
        self.MetaSuffix = self.MetaName = self.MetaPath = self.MetaRelpath = None

        assert path.startswith(location)
        relpath = path[len(location):]
        while relpath and relpath[0] == "/":
            relpath = relpath[1:]
        self.Relpath = relpath              # path relative to the location root, with leading slash removed
        
    def metaDescriptor(self):
        return FileDescriptor(self.Server, self.Location, self.MetaPath, self.MetaName, 0)
        
    def setMetaSuffix(self, suffix):
        self.MetaSuffix = suffix
        self.MetaName = self.Name + suffix
        self.MetaPath = self.Path + suffix
        self.MetaRelapth = self.Relpath + suffix
        return self
       
    def __str__(self):
        return "%s:%s" % (self.Server, self.Path)
        
    __repr__ = __str__
