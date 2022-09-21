Miscellaneous Tools
===================

File Declaration Tool
---------------------

File declaration tool accepts metadata JSON file produced by ProtoDUNE DAQ (as of June 2022) and declares the file to MetaCat.

Example of the input JSON file:

        .. code-block:: json

                {
                  "DUNE.campaign": "dc4",
                  "DUNE.datataking": "COLDBOX_run2021",
                  "checksum": "727689a8",
                  "data_stream": "test",
                  "data_tier": "raw",
                  "file_format": "binary",
                  "file_name": "data_406171931_2.test",
                  "file_size": 3168006718,
                  "file_type": "detector",
                  "runs": [
                    [
                      406171931,
                      1,
                      "dc4-vd-coldbox-top"
                    ]
                  ]
                }

Using the tool:

        .. code-block:: shell
        
                $ python tools/declare_meta.py 

                python declare_meta.py [options] <dataset namespace>:<dataset name> <metadata_file.json> ...

                Declares a file to MetaCat using the JSON file produced by ProtoDUNE DAQ (June 2022)

                Dataset namespace, name - MetaCat dataset to add files to

                Options:
                    -d                          - dry run - do not declare files to MetaCat. Just print the results of the metadata conversion to stdout
                    -n <namespace>              - file namespace, default - run type for first run in the metadata
                    -m <MetaCat URL>            - default - METACAT_SERVER_URL environment variable value      
                    -o (-|<output JSON file>)   - output file to write the resulting information, "-" means stdout
                    -e <file.json>              - metadata to add/override, optional
                    -p <did>[,...]              - parent files specified with their DIDs (<namespace>:<name>) or just <name>s if -n is used
                    -P <fid>[,...]              - parent files specified with their MetaCat file ids

The tool declares one or more files to MetaCat and adds all of them to the specified MetaCat dataset, which mush exist already.
The tool will read the metadata for each file as produced by the DAQ, convert it into format usable by MetaCat and declare the file then
add all these files to the specified dataset.

The namespace for each file can be specified using ``-n`` option, or the run type for the first run found in the file metadata will be used.
If specified with ``-n``, the namespace will also be used as the default namespace for parent files.

``-e`` option can be used to specify additional metadata for the files. If used, the specified JSON file will be parsed and then the metadata from the file
will be used to add or override metadta read from the input file. The following fields will not be affected:

        .. code-block::
        
            "file_name"
            "file_size"
            "checksum"
            "event_count"
            "file_type"
            "file_format"
            "data_tier"
            "data_stream"
            "events"
            "first_event"
            "last_event"
            "event_count"
                

For example:

        .. code-block:: shell
        
                $ cat meta.json
                {
                  "DUNE.campaign": "dc4",
                  "DUNE.datataking": "COLDBOX_run2021",
                  "checksum": "727689a8",
                  "data_stream": "test",
                  "data_tier": "raw",
                  "file_format": "binary",
                  "file_name": "data_406171931_3.test",
                  "file_size": 3168006718,
                  "file_type": "detector",
                  "runs": [
                    [
                      406171931,
                      1,
                      "dc4-vd-coldbox-top"
                    ]
                  ],
                  "events": [ 7,8,9 ]
                }
                
                $ cat extra.json 
                {
                   "math.pi": 3.14,
                   "math.primes": [2,3,5,7,11,13]
                }
                
                $ python tools/declare_meta.py -n declad_test -o - -e extra.json declad_test:test meta.json
                [
                    {
                        "fid": "72079136da3e43fa81ed27c99fcd527e",
                        "name": "data_406171931_3.test",
                        "namespace": "declad_test"
                    }
                ]
                
                $ metacat file show -j declad_test:data_406171931_3.test
                {
                    "checksums": {
                        "adler32": "727689a8"
                    },
                    "children": [],
                    "created_timestamp": 1655492877.700407,
                    "fid": "72079136da3e43fa81ed27c99fcd527e",
                    "metadata": {
                        "DUNE.campaign": "dc4",
                        "DUNE.datataking": "COLDBOX_run2021",
                        "core.data_stream": "test",
                        "core.data_tier": "raw",
                        "core.event_count": 3,
                        "core.events": [
                            7,
                            8,
                            9
                        ],
                        "core.file_format": "binary",
                        "core.file_type": "detector",
                        "core.run_type": "dc4-vd-coldbox-top",
                        "core.runs": [
                            406171931
                        ],
                        "core.runs_subruns": [
                            40617193100001
                        ],
                        "math.pi": 3.14,
                        "math.primes": [
                            2,
                            3,
                            5,
                            7,
                            11,
                            13
                        ]
                    },
                    "name": "data_406171931_3.test",
                    "namespace": "declad_test",
                    "parents": [],
                    "size": 3168006718
                }
                

