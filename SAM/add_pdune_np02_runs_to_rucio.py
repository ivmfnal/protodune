#! /usr/bin/env python

import sys
import collections
import urllib.parse
import re

import samweb_client

from rucio.client.replicaclient import ReplicaClient
from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient
from rucio.common.exception import DataIdentifierNotFound, DataIdentifierAlreadyExists, FileAlreadyExists, DuplicateRule, RucioException

samweb = samweb_client.SAMWebClient('dune')

def _chunk(iterator, chunksize): 
    """ Helper to divide an iterator into chunks of a given size """ 
    from itertools import islice 
    while True: 
        l = list(islice(iterator, chunksize)) 
        if l: yield l 
        else: return 

def convert_enstore_to_adler32(encrc, filesize):
    """ convert an enstore checksum to an adler32 one """
    if isinstance(encrc, str):
        try:
            tp, encrc = encrc.split(':',1)
            if tp != 'enstore':
                raise Exception('Unable to convert %s checksum to adler32' % tp)
        except ValueError:
            pass # no ':'
    try:
        encrc = int(encrc)
    except ValueError:
        raise ChecksumError("Invalid enstore checksum value: %s" % encrc)

    BASE = 65521
    sz = int(filesize % BASE)
    s1 = (encrc & 0xffff)
    s2 = ((encrc >> 16) & 0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (sz + s2) % BASE
    adler32 = (s2 << 16) + s1
    return '%08x' % adler32

FileInfo = collections.namedtuple('FileInfo', 'url file_name file_size adler32 md5')

def get_sam_files(defname, run_number):

    i = 0

    if run_number is not None:
        filelist = samweb.listFilesAndLocations(dimensions='defname: %s and run_number %d' % (defname, run_number), filter_path=['enstore:', 'cern-eos:', 'castor:'], checksums=True, schema=['gsiftp', 'root'])
    else:
        filelist = samweb.listFilesAndLocations(defname=defname, filter_path=['enstore:', 'cern-eos:', 'castor:'], checksums=True, schema=['gsiftp', 'root'])

    for loc in filelist:
        url, samloc, file_name, file_size, checksums = loc
        if isinstance(file_size, str):
            file_size = int(file_size)
        adler32 = md5 = None
        checksums = dict( c.split(':', 1) for c in checksums )
        try:
            adler32 = checksums['adler32']
        except KeyError:
            ecrc = checksums.get('enstore')
            if ecrc is not None:
                adler32 = convert_enstore_to_adler32(ecrc, file_size)
        md5 = None #checksums.get('md5')
        yield FileInfo(url, file_name, file_size, adler32, md5)
        i+=1
        if i > sys.maxsize: return

rse_map = { 'fndca1.fnal.gov': 'FNAL_DCACHE',
        'eospublicftp.cern.ch' : 'CERN_PDUNE_EOS',
        'eosctapublic.cern.ch' : 'CERN_PDUNE_CASTOR',
        } 

def add_rule(ruleclient, rucio_dataset, rse, rucio_account):
    # add a rule
    try:
        if not dry_run:
            ruleclient.add_replication_rule([{'scope': scope, 'name' : rucio_dataset}], 1, rse, ignore_availability=True, account=rucio_account)
            print('%s replication rule added for %s:%s' % (rse, scope, rucio_dataset))
        else:
            print('Would add %s replication rule for %s:%s' % (rse, scope, rucio_dataset))
    except DuplicateRule:
        print('%s replication rule already exists' % rse)

def add_run(samdef, run_number, rucio_account, scope, rucio_run_dataset, rucio_container, filter_runs, stream_container=None):

    print('Adding run number %d' % run_number)

    rucio_dataset = rucio_run_dataset.format(run_number=run_number)

    root_replicaclient = ReplicaClient(account='root') # with the current permission scheme root can add replicas anywhere
    didclient = DIDClient(account=rucio_account)
    ruleclient = RuleClient(account=rucio_account)


    knowndids = set()

    init_dataset = False
    eos_rule = False
    castor_rule = False
    fnal_rule = False

    data_stream = None

    found_files = False
    declared = None
    attached = None

    for filelist in _chunk(get_sam_files(samdef, run_number), 100):

        found_files = True
        if not init_dataset:
            if not dry_run:

                try:
                    #didclient.add_dataset(scope, rucio_dataset, rules=[{'copies' : 1, 'rse_expression': 'FNAL_DCACHE|CERN_PDUNE_EOS', 'account' : rucio_account, 'ignore_availability': True}])
                    didclient.add_dataset(scope, rucio_dataset)
                except DataIdentifierAlreadyExists:
                    dataset_contents = didclient.list_content(scope, rucio_dataset)
                    knowndids.update( (did["scope"],did["name"]) for did in dataset_contents )

            else:
                print('Would add dataset %s:%s' % (scope, rucio_dataset))
                dataset_contents = didclient.list_content(scope, rucio_dataset)
                knowndids.update( (did["scope"],did["name"]) for did in dataset_contents )
            init_dataset = True

        replicas_to_add = collections.defaultdict(list)
        dids_to_add = set()
        for fi in filelist:

            if stream_container and not data_stream:
                # lookup the datastream (it will be the same for the entire run)
                md = samweb.getMetadata(fi.file_name)
                data_stream = md.get('data_stream')

            pfn = fi.url
            print(pfn)
            parsed_url = urllib.parse.urlparse(pfn)
#            print parsed_url
            if parsed_url.hostname == 'castorpublic.cern.ch':
                rse = rse_map['eosctapublic.cern.ch']
                pfn = pfn.replace('root://castorpublic.cern.ch//castor/cern.ch/','root://eosctapublic.cern.ch/eos/ctapublic/archive/',1)
            else:
                rse = rse_map[parsed_url.hostname]
            if parsed_url.scheme == 'gsiftp' and parsed_url.port == 2811:
                #remove the default port
                url_components = list(parsed_url)
                url_components[1] = parsed_url.hostname
                pfn = urllib.parse.urlunparse(url_components)

            print('DID %s:%s' % (scope, fi.file_name))
            print(' PFN: %s' % pfn)
            print(' size; adler32, md5: %s %s %s' % (fi.file_size, fi.adler32, fi.md5))

            file_info = { 'scope' : scope, 'name' : fi.file_name, 'bytes' : fi.file_size, 'pfn' : pfn }
            if fi.adler32 is not None:
                file_info['adler32'] = fi.adler32
            if fi.md5 is not None:
                file_info['md5'] = fi.md5
            replicas_to_add[rse].append( file_info )
            dids_to_add.add( ( scope, fi.file_name ) )
#        print replicas_to_add['CERN_PDUNE_CASTOR']

        if replicas_to_add:
            if not dry_run:

                for rse in replicas_to_add:
                    print("rse, replicas to add")
                    print(rse)
                    print(replicas_to_add[rse])
                    print("knowndids")
                    print(knowndids)
                    # check for existing
                    possible_replicas = knowndids.intersection( (r["scope"], r["name"]) for r in replicas_to_add[rse])
                    if possible_replicas:
                        print("possible replicas")
                        print(possible_replicas)
                        existing_replicadata = list(root_replicaclient.list_replicas([{"scope":a[0], "name": a[1]} for a in possible_replicas], rse_expression=rse, all_states=True))
                        print("existing_replicadata")
                        print(existing_replicadata)
                        existing_replicas = set( (r["scope"], r["name"]) for r in existing_replicadata if r["rses"])
                        unavailable_replicas = set()
                        unavailable_replicas = set( (r["scope"], r["name"]) for r in existing_replicadata if "states" in r and r["states"].get(rse) == 'UNAVAILABLE' )
                        if unavailable_replicas:
                            # fix up a broken entries
                            # this isn't the most efficient way of doing this, but it should be rare
                            #replicas_to_update = []
                            #for r in replicas_to_add[rse]:
                            #    if (r["scope"], r["name"]) in unavailable_replicas:
                            #        replicas_to_update.append( r )
                            #        replicas_to_update[-1]["state"] = 'A'
                            #print rse, replicas_to_update
                            #root_replicaclient.update_replicas_states(rse, replicas_to_update[0:1] )

                            # remove unavailable replicas
                            for r in unavailable_replicas:
                                print('Replica %s:%s is marked unavailable on %s' % (r[0], r[1], rse))
                            root_replicaclient.delete_replicas(rse, [{"scope": scope, "name": name} for scope, name in unavailable_replicas])
                            print('Removed %d unavailable replicas from %s' % (len(unavailable_replicas), rse))

                            # and readd them
                            existing_replicas -= unavailable_replicas

                        replicas_to_add[rse] = [ r for r in replicas_to_add[rse] if (r["scope"], r["name"]) not in existing_replicas ]
                    if replicas_to_add[rse]:
                        ret = root_replicaclient.add_replicas(rse, replicas_to_add[rse])
                        declared = declared if declared is not None else True and bool(ret)
                    if rse == 'CERN_PDUNE_EOS' and not eos_rule:
                        # flag so we create a rule at the end pinning this to EOS
                        # (defer to end to avoid a race where some files at FNAL haven't been added to EOS yet, so Rucio creates a placeholder)
                        eos_rule = True
                    if rse == 'CERN_PDUNE_CASTOR' and not castor_rule:
                        castor_rule = True
                    if rse == 'FNAL_DCACHE' and not fnal_rule:
                        fnal_rule = True

                dids_to_add -= knowndids
                if dids_to_add:
                    ret = didclient.attach_dids(scope, rucio_dataset, [{"scope":a[0], "name": a[1]} for a in dids_to_add])
                    knowndids.update(dids_to_add)
                    attached = bool(ret)
                print("    --- declared: %s attached: %s" % (sum( len(rs) for rs in replicas_to_add.values()), len(dids_to_add)))
            else:
                print("    --- would declare %s files" % ", ".join( "%s : %d" % (rse, len(flist)) for rse, flist in replicas_to_add.items()))
                declared = True

        untagged_files = samweb.listFiles('file_name %s minus full_path "rucio:%s"' % ( ','.join(fi.file_name for fi in filelist), scope))
        for ut in untagged_files:
            if not dry_run:
                print("    --- added sam location rucio:%s to %s" % (scope, ut))
                samweb.addFileLocation(ut, "rucio:%s" % scope)
            else:
                print("    --- would add sam location rucio:%s to %s" % (scope, ut))

    if not declared:
        print('No files declared')

    if found_files:
        if eos_rule:
            add_rule(ruleclient, rucio_dataset, 'CERN_PDUNE_EOS', rucio_account)
        if castor_rule:
            add_rule(ruleclient, rucio_dataset, 'CERN_PDUNE_CASTOR', rucio_account)
        if fnal_rule:
            add_rule(ruleclient, rucio_dataset, 'FNAL_DCACHE', rucio_account)

        containers = []
        if rucio_container and run_number not in filter_runs:
            containers.append(rucio_container)
        if stream_container and run_number not in filter_runs:
            if not data_stream and declared:
                print('No stream available for run %s' % run_number)
            else:
                c = stream_container.get(data_stream)
                if c:
                    containers.append(c)


        for rucio_container in containers:
            if not dry_run:
                try:
                    didclient.add_datasets_to_container(scope, rucio_container, [{"scope": scope, "name": rucio_dataset}])
                    print('Added dataset %s:%s to container %s:%s' % (scope, rucio_dataset, scope, rucio_container))
                except RucioException as ex:
                    if 'already exists' in ex.args[0][0]:
                        print('Dataset %s:%s is already in container %s::%s' % (scope, rucio_dataset, scope, rucio_container))
                    else:
                        raise
            else:
                print('Would add dataset %s:%s to container %s:%s' % (scope, rucio_dataset, scope, rucio_container))

dry_run = False

if __name__ == '__main__':

    data_type = sys.argv[1]

    filter_runs = []
    if data_type == 'np04_raw':
        samdef = 'np04_raw_allruns'
        #rucio_container = 'np04_raw_all_run_numbers'
        rucio_container = None
        rucio_run_dataset = 'np04_raw_run_number_{run_number:d}'
        scope = 'protodune-sp'
        filter_runs = {825,826,830}
        stream_container = { 'calibration': 'raw_calibration_v0' }
    elif data_type == 'np02_raw':
        samdef = 'np02_raw_allruns'
        rucio_container = None
        rucio_run_dataset = 'np02_raw_run_number_{run_number:d}'
        scope = 'protodune-dp'
        stream_container = { 'calibration': 'raw_calibration_v0' }
    elif data_type.startswith('np04_full-reconstructed_'):
        m = re.match('np04_full-reconstructed_(v[0-9_]*)_[^_]+$', data_type)
        if not m:
            sys.exit('Unable to parse dataset name')
        samdef = data_type
        rucio_container = data_type
        rucio_run_dataset = 'np04_reco_keepup.'+ m.group(1) + '.run{run_number:06d}'
        scope = 'np04_reco_keepup'
        stream_container = {}
    elif data_type.startswith('np04_PDSPProd2_full-reconstructed_'):
        m = re.match('np04_PDSPProd2_full-reconstructed_(v[0-9_]*)_online_good_runs_\d?[.]?\dGeV+$', data_type)
        if not m:
            sys.exit('Unable to parse dataset name')
        samdef = data_type
        rucio_container = data_type
        rucio_run_dataset = 'np04_pdspprod2_reco.'+ m.group(1) + '.run{run_number:06d}'
        scope = 'np04_pdspprod2_reco'
        stream_container = {}
    elif data_type.startswith('PDSPProd2_MC_'):
        m = re.match('PDSPProd2_MC_\d?[.]?\dGeV_reco_sce_[^_]+$', data_type)
        if not m:
            sys.exit('Unable to parse dataset name')
        samdef = data_type
        rucio_container = data_type
        rucio_run_dataset = 'np04_pdspprod2_mc.run{run_number:06d}'
        scope = 'np04_pdspprod2_mc'
        stream_container = {}
    else:
        sys.exit('Unknown input data %s' % data_type)

    rucio_account = 'dunepro'

    run_range = sys.argv[2].split('-')

    if len(run_range) == 1:
        add_run(samdef, int(run_range[0]), rucio_account, scope, rucio_run_dataset, rucio_container, filter_runs=filter_runs, stream_container=stream_container)
    elif len(run_range) == 2:
        for run_number in range(int(run_range[0]), int(run_range[1])+1):
            add_run(samdef, run_number, rucio_account, scope, rucio_run_dataset, rucio_container, filter_runs=filter_runs, stream_container=stream_container)


