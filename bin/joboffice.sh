#!/bin/sh

# These arguments are in the same location for launchPipeline.sh and are
# reused here
DC3ROOT=/lsst/DC3root
RUNID=$2
VERBOSITY=$4
LOGDIRNAME=$6

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

(nohup joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME/.. $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/dev/null 2>&1 &) &
(nohup joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME/.. $DATAREL_DIR/pipeline/ISR/cfht-isr-joboffice.paf >/dev/null 2>&1 &) &
sleep 2
echo done starting job offices
exit 0
