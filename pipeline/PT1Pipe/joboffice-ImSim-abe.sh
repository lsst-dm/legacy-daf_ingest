#!/bin/sh

# These arguments are in the same location for launchPipeline.sh and are
# reused here
RUNID=$2
VERBOSITY=$4
LOGDIRNAME=$6

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/PT1Pipe/jobOffice-ImSim.paf >/dev/null 2>&1 &
# joboffice.py -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/PT1Pipe/jobOffice-ImSim.paf > /tmp/seeme44 2>&1 &

echo done starting job office
wait
