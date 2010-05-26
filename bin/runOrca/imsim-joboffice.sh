#!/bin/sh

# These arguments are in same location for launchPipeline.sh and are reused here
defaultRoot=/lsst/DC3/data/datarel/ImSim
RUNID=$2
VERBOSITY=$4

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/ISR/imsim-isr-joboffice.paf > /tmp/JOisr.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/CcdAssembly/imsim-ca-joboffice.paf >/tmp/JOca.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/CrSplit/imsim-crSplit-joboffice.paf >/tmp/JOcrSplit.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/ImgChar/imsim-imgChar-joboffice.paf >/tmp/JOimgChar.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/SFM/imsim-sfm-joboffice.paf >/tmp/JOsfm.log 2>&1 &) &

sleep 2
echo done starting job offices
exit 0
