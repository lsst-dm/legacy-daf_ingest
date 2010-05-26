#!/bin/sh

if [ "x$DATAREL_DIR" == "x" ]; then
   echo "You must 'setup datarel' before use."
   exit 0
fi

if [ $# -lt 2 ] ; then
   echo "imsim-buildRunCommands.sh <runid> <inputlist>\nwhere\n   runid : unique ID for run\n    inputlist : full pathname to list of visit/snap/raft/sensor/channel to process.\nExample: imsim-buildRunCommands.sh raa20100521_01 /tmp/TestImsimInput.txt"
   exit 0
fi


echo -e "\n"
cd $DATAREL_DIR; echo moving into: $DATAREL_DIR
echo -e "============================\n"
echo "     cd $DATAREL_DIR/pipeline; orca.py -r $DATAREL_DIR/pipeline -e $DATAREL_DIR/bin/runOrca/imsim-setupForOrcaUse.sh -V 10 -P 10 imsim-orca.paf $1 "
echo -e "============================\n"
echo "     cd $DATAREL_DIR/pipeline; announceDataset.py -r $1 -b lsst8.ncsa.uiuc.edu -t RawAvailable $2"
echo -e "===========================\n"
echo "     shutprod.py 1 $1"
echo -e "===========================\n"

