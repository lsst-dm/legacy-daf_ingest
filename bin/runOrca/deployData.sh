#! /bin/sh

# E.G. /lsst/DC3/data/datarel/CFHTLS/D3/%(runid)s
RunIdDir=$1

# e.g. /lsst/DC3/data/obstest
repository=$2

# e.g. CFHTLS/D3
coll=$3

set=`basename $coll`

pipelines="ISR"   # insert newlines to get array behavior

if [ "$coll" == "ImSim" ]; then
  mkdir -p $RunIdDir/input
  [ -e "$RunIdDir/input/raw"  ] || ln -s $repository/$coll/raw  $RunIdDir/input/raw
  [ -e "$RunIdDir/input/dark" ] || ln -s $repository/$coll/dark $RunIdDir/input/dark
  [ -e "$RunIdDir/input/bias" ] || ln -s $repository/$coll/bias $RunIdDir/input/bias
  [ -e "$RunIdDir/input/flat" ] || ln -s $repository/$coll/flat $RunIdDir/input/flat
  [ -e "$RunIdDir/input/registry.sqlite3" ] || ln -s $repository/registry.sqlite3 $RunIdDir/input/registry.sqlite3
else
  mkdir -p $RunIdDir/input/$set
  [ -e "$RunIdDir/input/$set/raw"  ] || ln -s $repository/$coll/raw  $RunIdDir/input/$set/raw
  [ -e "$RunIdDir/input/bias" ] || ln -s $repository/$coll/../calib/bias $RunIdDir/input/bias
  [ -e "$RunIdDir/input/flat" ] || ln -s $repository/$coll/../calib/flat $RunIdDir/input/flat
  [ -e "$RunIdDir/input/registry.sqlite3" ] || ln -s $repository/$coll/../registry.sqlite3 $RunIdDir/input/registry.sqlite3
  [ -e "$RunIdDir/input/calibRegistry.sqlite3" ] || ln -s $repository/$coll/../calib/calibRegistry.sqlite3 $RunIdDir/input/calibRegistry.sqlite3
fi  
