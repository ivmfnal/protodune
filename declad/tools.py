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
    #if debug:
    #    debug("%s [%s] [%s]" % (status, out, err))
        
    if not out: out = err
    
    if status is None:
        cmd.kill()
        out = (out or "") + "\n subprocess timed out\n"
        status = 100
    
    return status, out

if __name__ == '__main__':

	command="xrdfs eospublic.cern.ch ls -l /eos/experiment/neutplatform/protodune/scratchdisk/daq/data"
	#command="ls -l /eos/experiment/neutplatform/protodune/scratchdisk/daq/data"

	import sys

	status, out = runCommand(command, timeout=10)
	#print out
	#print status
    
