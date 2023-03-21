import hashlib
from datetime import datetime

def lfn2pfn_dune(namespace, filename, metadata):
    # determine year from timestamps
    timestamp = metadata.get('core.start_time') or metadata.get('core.end_time')
    year = None if timestamp is None else datetime.utcfromtimestamp(timestamp).year

    # determine hashes from run number
    run_number = int(metadata.get('core.runs', [0])[0])
    run_number = ("%08d" % (run_number,))[-8:]                  # keep lower 8 digits
    hash1 = run_number[0:2]
    hash2 = run_number[2:4]
    hash3 = run_number[4:6]
    hash4 = run_number[6:8]

    run_type = metadata.get('core.run_type')
    data_tier = metadata.get('core.data_tier')
    file_type = metadata.get('core.file_type')
    data_stream = metadata.get('core.data_stream')
    data_campaign = metadata.get('DUNE.campaign')

    pfn = "/".join([str(x) for x in 
        [run_type, data_tier, year, file_type, data_stream, data_campaign, hash1, hash2, hash3, hash4, filename]
    ])
    return pfn

def lfn2pfn_hash(scope, name, metadata):
    """
    From Rucio lib/rucio/rse/protocols/protocol.py
    """
    hstr = hashlib.md5(('%s:%s' % (scope, name)).encode('utf-8')).hexdigest()
    if scope.startswith('user') or scope.startswith('group'):
        scope = scope.replace('.', '/')
    return '%s/%s/%s/%s' % (scope, hstr[0:2], hstr[2:4], name)

def lfn2pfn(algorithm, scope, name, metadata):
    algorithm = algorithm or "hash"
    if algorithm == "hash":
        return lfn2pfn_hash(scope, name, metadata)
    elif algorithm == "dune":
        return lfn2pfn_dune(scope, name, metadata)
    else:
        raise ValueError("Uknown LFN2PFN algorithm: %s" % (algorithm,))
