#!/bin/bash

## These arguments are in the same location for launchPipeline.sh and are
## reused here
#DC3ROOT=/u/ac/srp/orca_scratch
#RUNID=$2
#VERBOSITY=$4
#
#if [ $VERBOSITY = "None" ]; then
#     VERBOSITY="silent"
#fi
#
#which joboffice.py
##(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst7.ncsa.uiuc.edu -d $DC3ROOT/$RUNID/work $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/tmp/srp1 2>&1 &) &
##(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst7.ncsa.uiuc.edu -d $DC3ROOT/$RUNID/work $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/tmp/srp1 2>&1 &) &
#joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $DC3ROOT/$RUNID/work $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/dev/null  2>&1 &
#joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $DC3ROOT/$RUNID/work $DATAREL_DIR/pipeline/ISR/cfht-isr-joboffice.paf >/dev/null  2>&1 &
#wait
##
##!/bin/sh

# These arguments are in the same location for launchPipeline.sh and are
# reused here
DC3ROOT=/u/ac/srp/orca_scratch
RUNID=$2
VERBOSITY=$4
LOGDIRNAME=$6

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/dev/null 2>&1 &
joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/ISR/cfht-isr-joboffice.paf >/dev/null 2>&1 &
echo done starting job offices
wait
