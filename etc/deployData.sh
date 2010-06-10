#! /bin/sh
RUNID=$1
repository=$2
coll=$3
pipelines="ISR"   # insert newlines to get array behavior

mkdir $RUNID/input/D2
[ -e "$RUNID/input/raw"  ] || ln -s $repository/$coll/raw  $RUNID/input/D2/raw
if [ "$coll" == "ImSim" ]; then
  [ -e "$RUNID/input/dark" ] || ln -s $repository/$coll/dark $RUNID/input/dark
  [ -e "$RUNID/input/bias" ] || ln -s $repository/$coll/bias $RUNID/input/bias
  [ -e "$RUNID/input/flat" ] || ln -s $repository/$coll/flat $RUNID/input/flat
  [ -e "$RUNID/input/registry.sqlite3" ] || ln -s $repository/registry.sqlite3 $RUNID/input/registry.sqlite3
else
  [ -e "$RUNID/input/dark" ] || ln -s $repository/$coll/../calib/dark $RUNID/input/dark
  [ -e "$RUNID/input/bias" ] || ln -s $repository/$coll/../calib/bias $RUNID/input/bias
  [ -e "$RUNID/input/flat" ] || ln -s $repository/$coll/../calib/flat $RUNID/input/flat
  [ -e "$RUNID/input/registry.sqlite3" ] || ln -s $repository/$coll/../registry.sqlite3 $RUNID/input/registry.sqlite3
  [ -e "$RUNID/input/calibRegistry.sqlite3" ] || ln -s $repository/$coll/../calib/calibRegistry.sqlite3 $RUNID/input/calibRegistry.sqlite3
fi  
