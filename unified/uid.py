from pythreader import Primitive, synchronized
import string, random, time


class _UIDGen(Primitive):
    
    def __init__(self, start=1, multiplier=1, offset=0, tag=""):
        Primitive.__init__(self)
        self.Range = 1000000
        self.Next = start
        self.Tag = tag
        self.Offset = offset
        self.Multiplier = multiplier
        self.T = int(time.time()) % 1000

    _alphabet=string.ascii_lowercase + string.ascii_uppercase

    @synchronized
    def get(self, as_int=False):
        u = self.Next
        self.Next = (self.Next + 1) % self.Range
        u = self.Next * self.Multiplier + self.Offset
        if not as_int:
            a1 = random.choice(self._alphabet)
            a2 = random.choice(self._alphabet)
            a3 = random.choice(self._alphabet)
            t = (self.T + u//1000)%1000
            u = "%03d.%s%s%s.%03d" % (t, a1, a2, a3, u%1000)
            if self.Tag:
                u = self.Tag + "." + u
        return u
            
        

_uid = _UIDGen()

def uid(u=None, as_int=False, tag=""):
    global _uid
    if u is not None:   return u
    u = _uid.get(as_int)
    return u
    
def init(multiplier=1, tag="", offset=0, start=1):
    global _uid
    _uid = _UIDGen(start, multiplier, offset, tag)
    
