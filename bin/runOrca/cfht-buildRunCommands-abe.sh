#!/bin/sh
if [ "x$DATAREL_DIR" == "x" ]; then
   echo -e "You must 'setup datarel' before use."
   exit 0
fi

if [ $# -lt 1 ] ; then
   echo -e "cfht-buildRunCommands.sh <runid> \nwhere\n   runid : unique ID for run\n   \nExample: cfht-buildRunCommands.sh raa20100521_01 "
   exit 0
fi

cat <<EOF
===========================
     orca.py -r pipeline -e $DATAREL_DIR/bin/runOrca/cfht-setupForOrcaUse-abe.sh -V 10 -P 3 pipeline/cfht-orca-abe.paf $1
===========================
     shutprod.py 1 $1
===========================
     killcondor.py pipeline/cfht-orca-abe.paf $1
     killcondor.py -g pipeline/cfht-orca-abe.paf $1
===========================
     condor_rm <condor q id>
===========================

EOF
