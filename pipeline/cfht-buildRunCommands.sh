if [ "x$DATAREL_DIR" == "x" ]; then
   echo "You must 'setup datarel' before use."
   exit 0
fi

cd $DATAREL_DIR; echo moving into: $DATAREL_DIR
echo "============================"
echo "     cd $DATAREL_DIR/pipeline; orca.py -r $DATAREL_DIR/pipeline -e cfht-setupForOrcaUse.sh -V 10 -P 10 cfht-orca.paf $1 "
echo "============================"
echo "     cd $DATAREL_DIR/pipeline; announceDataset.py -r $1 -b lsst8.ncsa.uiuc.edu -t RawAvailable $DATAREL_DIR/pipeline/ISR/cfht-isr-inputdata.txt"
echo "==========================="
