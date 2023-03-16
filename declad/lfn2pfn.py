import hashlib

def lfn2pfn_dune(namespace, filename, metadata, attributes):
    # determine year from timestamps
    timestamp = metadata.get('core.start_time') \
        or metadata.get('core.end_time') \
        or attributes.get('created_timestamp')
    if timestamp is None:
        year = 'None'
    else:
        dt = datetime.utcfromtimestamp(timestamp)
        year = dt.year

    # determine hashes from run number
    run_number = int(metadata.get('core.runs', [0])[0])
    run_number = ("%08d" % (run_number,))[-8:]                  # keep lower 8 digits
    hash1 = run_number[0:2]
    hash2 = run_number[2:4]
    hash3 = run_number[4:6]
    hash4 = run_number[6:8]

    run_type = metadat.get('core.run_type')
    data_tier = metadat.get('core.data_tier')
    file_type = metadat.get('core.file_type')
    data_stream = metadat.get('core.data_stream')
    data_campaign = metadat.get('DUNE.campaign')

    pfn = "/".join([str(x) for x in 
        [run_type, data_tier, year, file_type, data_stream, data_campaign, hash1, hash2, hash3, hash4, filename]
    ]
    return pfn

def lfn2pfn_hash(scope, name, metadata, attributes):
    """
    From Rucio lib/rucio/rse/protocols/protocol.py
    """
    hstr = hashlib.md5(('%s:%s' % (scope, name)).encode('utf-8')).hexdigest()
    if scope.startswith('user') or scope.startswith('group'):
        scope = scope.replace('.', '/')
    return '%s/%s/%s/%s' % (scope, hstr[0:2], hstr[2:4], name)

def lfn2pfn(algorithm, scope, name, metadata, attributes):
    algorithm = algorithm or "hash"
    if algorithm == "hash":
        return lfn2pfn_hash(scope, name, metadata, attributes)
    elif algorithm = "dune":
        return lfn2pfn_dune(scope, name, metadata, attributes)
    else:
        raise ValueError("Uknown LFN2PFN algorithm: %s" % (algorithm,))
