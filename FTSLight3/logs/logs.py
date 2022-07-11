import traceback, sys, time
from datetime import datetime
from .log_file import LogFile, LogStream

DefaultLogger = None

class LogChannel(object):
    
    def __init__(self, output, label=None, enabled=True, timestamps=True):
        self.Timestamps = timestamps
        self.Output = LogFile(output) if isinstance(output, str) else (
            LogStream(output) if output in (sys.stdout, sys.stderr)
            else output
        )            # Either LogFile or LogStream or path or stdout or stderr
        self.Label = label
        self.Enabled = enabled
        
    def enable(self, enabled=True):
        self.Enabled = enabled

    def log(self, who, *message, sep=" ", t=None, label=None):
        #print("LogChannel.log(): who:", who)
        if self.Enabled:
            message = sep.join([str(p) for p in message])
            label = label or self.Label
            if label is not None:
                message = f"[{label}] {message}"
            if who:
                message = f"{who}: {message}"
            if not self.Timestamps: t = False
            self.Output.log(message, t=t)


class AbstractLogger(object):

    def log(self, *message, sep=" ", who=None, t=None, channel="log"):
        raise NotImplementedError()


class Logger(AbstractLogger):

    def __init__(self, log_path, error_path=None, debug_path=None, debug=True, append=True):
        self.Debug = debug
        log_output = self.make_output(log_path, sys.stdout, append=append)
        
        # default channels
        self.Channels = {       
            "log":      LogChannel(log_output),
            "error":    LogChannel(log_output if error_path is None else self.make_output(error_path, sys.stderr, append=append), label="ERROR")
        }
        if debug:
            self.Channels["debug"] = LogChannel(log_output if debug_path is None else self.make_output(debug_path, sys.stderr, append=append), label="DEBUG")

    def add_channel(self, name, path=None, print_label=False, timestamps=True, **params):
        log_out = self.Channels["log"].Output
        self.Channels[name] = LogChannel(log_out if path is None else self.make_output(path, sys.stdout, **params), 
                label = name if print_label else None,
                timestamps = timestamps
        )

    def make_output(self, output, dash_stream, **params):
        if output is None:  
            return None
        elif isinstance(output, (LogFile, LogStream)):
            return output
        elif output == "-":
            return LogStream(dash_stream)
        elif output is sys.stderr or output is sys.stdout:
            return LogStream(output)
        else:
            #print("Logger.__init__: output:", output)
            out = LogFile(output, **params)
            out.start()
            return out

    def log(self, *message, sep=" ", who=None, t=None, channel="log"):
        #print("Logger.log(", message, sep, who, t, channel, ")")
        assert who is not None, "Message originator (who) must be specified"
        channel = self.Channels.get(channel)
        if channel is not None:
            channel.log(who, *message, sep=sep, t=t)

    def error(self, *message, sep=" ", who=None, t=None):
        assert who is not None, "Message originator (who) must be specified"
        self.log(*message, channel="error", sep=sep, who=who, t=t)

    def debug(self, *message, sep=" ", who=None, t=None):
        assert who is not None, "Message originator (who) must be specified"
        if self.Debug:
            self.log(*message, channel="debug", sep=sep, who=who, t=t)


class Logged(AbstractLogger):

    def __init__(self, name=None, debug=True, logger=None):
        assert logger is None or isinstance(logger, AbstractLogger), "logger must be either None or a Logger or a Logged"
        self.Logger = logger
        self.LogName = name or self.__class__.__name__
        self.Debug = debug
        #print("Logged: LogName=", self.LogName)

    def log(self, *message, sep=" ", who=None, t=None, channel="log"):
        #print("Logged.log(", message, sep, who, t, channel, ")")
        who = who or self.LogName
        logger = self.Logger or DefaultLogger
        if logger is not None and (channel != "debug" or self.Debug):
           logger.log(*message, sep=sep, who=who, t=t, channel=channel)
    
    def error(self, *message, sep=" ", who=None, t=None):
        self.log(*message, sep=sep, who=who or self.LogName, t=t, channel="error")

    def debug(self, *message, sep=" ", who=None, t=None):
        self.log(*message, sep=sep, who=who or self.LogName, t=t, channel="debug")


def init(log_output, error_out=None, debug_out=None, debug_enabled=False):
    global DefaultLogger
    DefaultLogger = Logger(log_output, error_out, debug_out, debug_enabled)
    return DefaultLogger
    
