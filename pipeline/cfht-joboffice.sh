#!/bin/sh

# These arguments are in same location for launchPipeline.sh and are reused here
defaultRoot=/lsst/DC3/data/datarel/CFHTLS/D3
RUNID=$2
VERBOSITY=$4

if [ $VERBOSITY = "None" ]; then
     VERBOSITY="silent"
fi

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/ISR/cfht-isr-joboffice.paf > /home/rallsman/isrJO.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/CcdAssembly/cfht-ca-joboffice.paf >/home/rallsman/caJO.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/CrSplit/cfht-crSplit-joboffice.paf >/home/rallsman/crSplit.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/ImgChar/cfht-imgChar-joboffice.paf >/home/rallsman/imgCharJO.log 2>&1 &) &

(joboffice.py -D -L $VERBOSITY -r $RUNID -b lsst8.ncsa.uiuc.edu -d $defaultRoot/$RUNID/work $DATAREL_DIR/pipeline/SFM/cfht-sfm-joboffice.paf >/home/rallsman/sfmJO.log 2>&1 &) &

sleep 2
echo done starting job offices
exit 0
