import traceback, sys, time
from pythreader import LogFile, LogStream

LogOut = ErrorOut = DebugOut = None
DebugEnabled = False

class Logged(object):

    def __init__(self, name=None, debug=None):
        self.LogName = name or self.__class__.__name__
        self.Debug = debug if debug is not None else DebugEnabled
        
    def debug(self, *params, sep=" "):
        if self.Debug and DebugOut is not None:
            DebugOut.log("%s [DEBUG]: %s" % (self.LogName, sep.join([str(p) for p in params])))

    def log(self, *params, sep=" "):
        if LogOut is not None:
            LogOut.log("%s: %s" % (self.LogName, sep.join([str(p) for p in params])))

    def error(self, *params, sep=" "):
        if ErrorOut is not None:
            ErrorOut.log("%s [ERROR]: %s" % (self.LogName, sep.join([str(p) for p in params])))

def make_log(output, dash_stream, **params):
    if output is None:  
        return None
    elif isinstance(output, (LogFile, LogStream)):
        return output
    elif output == "-":
        return LogStream(dash_stream)
    elif output is sys.stderr or output is sys.stdout:
        return LogStream(output)
    else:
        out = LogFile(output, **params)
        out.start()
        return out
            
def init_logger(log_output, debug_enabled=False, debug_out=None, error_out=None):
    global LogOut, ErrorOut, DebugOut, DebugEnabled
    LogOut = make_log(log_output, sys.stdout)
    ErrorOut = make_log(error_out, sys.stderr)
    DebugOut = None if not debug_enabled else make_log(debug_out, sys.stdout)
    DebugEnabled = debug_enabled
    
