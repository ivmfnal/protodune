#!/bin/bash

#xrdfs eospublic.cern.ch ls /eos/experiment/neutplatform/protodune/dune/test/daq

server=eospublic.cern.ch
t=$$
postfix=${1:-$t}
path=/eos/experiment/neutplatform/protodune/dune/test/daq/data_${postfix}.hdf5
json_path=${path}.json

xrdcp mover.log xroot://${server}/$path
xrdcp meta.json xroot://${server}/$json_path

echo created: $path

