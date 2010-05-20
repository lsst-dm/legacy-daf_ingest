if [ "x$DATAREL_DIR" == "x" ]; then
   echo "You must 'setup datarel' before use."
   exit 0
fi

echo -e "\n"
cd $DATAREL_DIR; echo moving into: $DATAREL_DIR
echo -e "============================\n"
echo "     cd $DATAREL_DIR/pipeline; orca.py -r $DATAREL_DIR/pipeline -e $DATAREL_DIR/pipeline/cfht-setupForOrcaUse.sh -V 10 -P 10 cfht-orca.paf $1 "
echo -e "============================\n"
echo "     cd $DATAREL_DIR/pipeline; announceDataset.py -r $1 -b lsst8.ncsa.uiuc.edu -t RawAvailable $DATAREL_DIR/pipeline/ISR/cfht-isr-inputdata.txt"
echo "==========================="
