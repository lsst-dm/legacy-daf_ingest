#! /bin/sh

# E.G. /lsst/DC3/data/datarel-test/CFHTLS/%(runid)s
RunIdDir=$1

# e.g. /lsst/DC3/data/obstest
repository=$2

# e.g. CFHTLS
coll=$3

[ -e $RunIdDir/input ] || \
    mkdir -p $RunIdDir/input && ln -s $repository/$coll/* $RunIdDir/input/
[ -e $RunIdDir/update ] || \
    mkdir -p $RunIdDir/update && \
    ln -s $repository/$coll/registry.sqlite3 $RunIdDir/update/registry.sqlite3
