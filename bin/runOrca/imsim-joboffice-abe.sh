#!/bin/sh

# These arguments are in same location for launchPipeline.sh and are reused here
defaultRoot=/cfs/projects/lsst/DC3/data/datarel-runs
RUNID=$2
VERBOSITY=$4
LOGDIRNAME=$6

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

joboffice.py  -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/ISR/imsim-isr-joboffice.paf > /dev/null 2>&1 &

#joboffice.py  -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/CcdAssembly/imsim-ca-joboffice.paf >/dev/null 2>&1 &

#joboffice.py  -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/CrSplit/imsim-crSplit-joboffice.paf >/dev/null 2>&1 &

#joboffice.py  -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/ImgChar/imsim-imgChar-joboffice.paf >/dev/null 2>&1 &

#joboffice.py  -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $LOGDIRNAME $DATAREL_DIR/pipeline/SFM/imsim-sfm-joboffice.paf >/dev/null 2>&1 &

echo done starting job offices
wait
