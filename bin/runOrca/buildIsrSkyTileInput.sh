#!/bin/sh

cfhtRegistry="/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"
imsimRegistry="/lsst/DC3/data/obstest/ImSim/registry.sqlite3"
error="Use:   buildIsrSkyTileInput.sh  -c | -i \nwhere \n    -c indicates cfht data\n    -i indicates ImSim data\nExample:  buildIsrInput.sh  -c\nExample:  buildIsrInput.sh  -i\n"

if [ $# -lt 1 ] ; then
    echo -e $error
    exit
fi
okToProcess=0
if [ "$1" == "-c" ]; then
    registry=$cfhtRegistry
    type="cfht"
    okToProcess=1
elif [ "$1" == "-i" ]; then
    registry=$imsimRegistry
    type="imsim"
    okToProcess=1
elif [ $okToProcess -eq 0 ]; then
    echo -e $error
    exit
fi


for i in `sqlite3 $registry "SELECT DISTINCT(skyTile) FROM raw_skyTile ORDER BY skyTile"` ; do
    echo $label Processing: $i
    ./SkyTileCcds.py $1 $i > $type-isr-skytileInput-$i.txt
done
