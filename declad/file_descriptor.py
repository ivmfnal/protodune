class FileDescriptor(object):

    def __init__(self, server, location, path, name, size):
        self.Server = server
        self.Location = location
        self.Name = name
        self.Size = size
        self.OrigPath = path
        assert path.startswith(location)
        relpath = path[len(location):]
        while relpath and relpath[0] == "/":
            relpath = relpath[1:]
        self.RelPath = relpath              # path relative to the location root, with leading slash removed

    def path(self, location):
        return location + "/" + self.RelPath

    def __str__(self):
        return "%s:%s:%s(%s)" % (self.Server, self.Location, self.RelPath, self.Size)
        
    __repr__ = __str__

