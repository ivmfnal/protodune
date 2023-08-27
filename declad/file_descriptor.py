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

