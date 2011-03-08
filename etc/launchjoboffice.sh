#!/bin/bash

RUNID=$2
VERBOSITY=$4
LOGDIRNAME=$6

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/dev/null 2>&1 &
pid1=$!
joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/ISR/cfht-isr-joboffice.paf >/dev/null 2>&1 &
pid2=$!
echo done starting job offices
wait $pid1 $pid2
exit $?
