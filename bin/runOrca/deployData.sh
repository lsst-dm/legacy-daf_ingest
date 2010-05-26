#! /bin/sh

# E.G. /lsst/DC3/data/datarel-test/CFHTLS/%(runid)s
RunIdDir=$1

# e.g. /lsst/DC3/data/obstest
repository=$2

# e.g. CFHTLS
coll=$3

set=`basename $coll`

pipelines="ISR"   # insert newlines to get array behavior

if [ "$coll" == "ImSim" ]; then
  mkdir -p $RunIdDir/input
  [ -e "$RunIdDir/input/raw"  ] || ln -s $repository/$coll/raw  $RunIdDir/input/raw
  [ -e "$RunIdDir/input/dark" ] || ln -s $repository/$coll/dark $RunIdDir/input/dark
  [ -e "$RunIdDir/input/bias" ] || ln -s $repository/$coll/bias $RunIdDir/input/bias
  [ -e "$RunIdDir/input/flat" ] || ln -s $repository/$coll/flat $RunIdDir/input/flat
  [ -e "$RunIdDir/input/registry.sqlite3" ] || ln -s $repository/$coll/registry.sqlite3 $RunIdDir/input/registry.sqlite3
else
  mkdir -p $RunIdDir/input
  [ -e "$RunIdDir/input/D1"  ] || ln -s $repository/$coll/D1  $RunIdDir/input/D1
  [ -e "$RunIdDir/input/D2"  ] || ln -s $repository/$coll/D2  $RunIdDir/input/D2
  [ -e "$RunIdDir/input/D3"  ] || ln -s $repository/$coll/D3  $RunIdDir/input/D3
  [ -e "$RunIdDir/input/D4"  ] || ln -s $repository/$coll/D4  $RunIdDir/input/D4
  [ -e "$RunIdDir/input/registry.sqlite3" ] || ln -s $repository/$coll/registry.sqlite3 $RunIdDir/input/registry.sqlite3
  [ -e "$RunIdDir/input/calib"  ] || ln -s $repository/$coll/calib  $RunIdDir/input/calib
fi  
