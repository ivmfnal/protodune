class FileDescriptor(object):

    def __init__(self, server, location, path, size):
        assert path.startswith(location)
        self.Server = server
        self.Location = location
        if '/' in path:
            self.Name = path.rsplit("/", 1)[-1]
        else:
            self.Name = path
        self.Size = size
        self.OrigPath = path
        relpath = path[len(location):]
        while relpath and relpath[0] == "/":
            relpath = relpath[1:]
        self.RelPath = relpath              # path relative to the location root, with leading slash removed

    def path(self, location):
        return location + "/" + self.RelPath

    def __str__(self):
        return "%s:%s:%s(%s)" % (self.Server, self.Location, self.RelPath, self.Size)
        
    __repr__ = __str__

