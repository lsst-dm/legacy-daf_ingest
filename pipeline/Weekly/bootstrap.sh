#!/bin/sh

if [ $# -eq 0 ]
then
echo "$0 : You must supply 'trunk' or 'tags' as a command line argument"
exit 1
fi


if [ $1 = 'tags' -o $1 = 'trunk' ]
then
echo "Running for stack: $1 "
else
echo "$1 : You must supply 'trunk' or 'tags' as a command line argument"
exit 1
fi


# grab the date for labelling the run
i=`date "+%Y_%m%d"`

# stackType="trunk" stackType="tags"
stackType=$1

if [ $stackType = 'tags' ]
then

export SHELL=/bin/bash
export LSST_HOME=/lsst/DC3/stacks/default
source /lsst/DC3/stacks/default/loadLSST.sh
setup datarel

else

export SHELL=/bin/bash
export LSST_DEVEL=/lsst/home/buildbot/buildbotSandbox
source /lsst/DC3/stacks/default/loadLSST.sh
setup datarel

fi

eups list | grep Setup

echo DATAREL_DIR $DATAREL_DIR

# ls $DATAREL_DIR/pipeline

echo "copy pipeline directory from DATAREL"
cp -r $DATAREL_DIR/pipeline .

echo "change to the pipeline directory"
cd pipeline 

echo "copy weekly production files"
cp Weekly/* .

echo "copy PT1Pipe policy"
cp PT1Pipe/main-ImSim.paf . 

echo "launch weekly production"
nohup ./run_weekly_production.sh ${stackType}  >& weekly_production_$i.log








