#!/bin/sh

# This script     run_weekly_production.sh   
# uses files
#          load_env_trunk.sh  or  load_env.tags.sh
#          stack_trunk.sh     or  stack_tags.sh
#          weekly_production.paf
#
# from the current working directory 


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

echo "Starting run_weekly_production"

# grab the date for labelling the run
i=`date "+%Y_%m%d_%H%M%S"`
echo $i

# Output directory space for the run:
# Production will make a directory 'thisrun' under base.
# 
base="/lsst3/weekly"
dbuser="buildbot"

# stackType="tags" stackType="trunk"
stackType=$1
echo "stackType ${stackType}"


# Runid for the weekly production 
thisrun="wp_${stackType}_$i"
echo "runID ${thisrun}"
echo "FullpathToWeeklyRun: ${base}/datarel-runs/${thisrun}"

echo "Running: source ./load_env_${stackType}.sh "
source ./load_env_${stackType}.sh

echo "Saving configuration in: ${base}/datarel-runs/${thisrun}/config/weekly.tags"
mkdir -p ${base}/datarel-runs/${thisrun}/config
eups list -s > ${base}/datarel-runs/${thisrun}/config/weekly.tags

echo PWD 
echo $PWD 

echo RUNNING 
echo "orca.py -r $PWD -e $PWD/stack_${stackType}.sh -V 30 -L 2 weekly_production.paf ${thisrun}"

orca.py -r $PWD -e $PWD/stack_${stackType}.sh -V 30 -L 2 weekly_production.paf ${thisrun} >& unifiedPipeline.log

cd ${base}/datarel-runs/${thisrun}

pwd

# $cmd = "${DATAREL_DIR}/bin/sst/SourceAssoc_ImSim.py -i update -o SourceAssoc/${skytile} -R update/registry.sqlite3 --skyTile=${skytile}";

# Run SourceAssoc
mkdir SourceAssoc
echo "${DATAREL_DIR}/bin/sst/SourceAssoc_ImSim.py -i update -o SourceAssoc -R update/registry.sqlite3";
${DATAREL_DIR}/bin/sst/SourceAssoc_ImSim.py -i update -o SourceAssoc -R update/registry.sqlite3 >& SourceAssoc_ImSim.log 

# Prepare DB
echo "${DATAREL_DIR}/bin/ingest/prepareDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu ${dbuser}_PT1_2_u_${thisrun}";
${DATAREL_DIR}/bin/ingest/prepareDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu ${dbuser}_PT1_2_u_${thisrun} >& prepareDb.log 

# Ingest processed metadata
echo "${DATAREL_DIR}/bin/ingest/ingestProcessed_ImSim.py -u ${dbuser} -d ${dbuser}_PT1_2_u_${thisrun} update update/registry.sqlite3";
${DATAREL_DIR}/bin/ingest/ingestProcessed_ImSim.py -u ${dbuser} -d ${dbuser}_PT1_2_u_${thisrun} update update/registry.sqlite3 >& ingestProcessed_ImSim.log

# Ingest source association data
mkdir csv-SourceAssoc
echo "${DATAREL_DIR}/bin/ingest/ingestSourceAssoc.py -m -u ${dbuser} -R /lsst/DC3/data/obs/ImSim/ref/simRefStar_04122011.csv -e /lsst3/weekly/datarel-runs/${thisrun}/Science_Ccd_Exposure_Metadata.csv -H lsst10.ncsa.uiuc.edu -j 1 ${dbuser}_PT1_2_u_${thisrun}  SourceAssoc  csv-SourceAssoc";
${DATAREL_DIR}/bin/ingest/ingestSourceAssoc.py -m -u ${dbuser} -R /lsst/DC3/data/obs/ImSim/ref/simRefStar_03052011.csv -e /lsst3/weekly/datarel-runs/${thisrun}/Science_Ccd_Exposure_Metadata.csv -H lsst10.ncsa.uiuc.edu -j 1 ${dbuser}_PT1_2_u_${thisrun}  SourceAssoc  csv-SourceAssoc >& ingestSourceAssoc.log 

# Run SQDA igestion script 
echo "${DATAREL_DIR}/bin/ingest/ingestSdqa_ImSim.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -d ${dbuser}_PT1_2_u_${thisrun}  update update/registry.sqlite3 ";
${DATAREL_DIR}/bin/ingest/ingestSdqa_ImSim.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -d ${dbuser}_PT1_2_u_${thisrun} update  update/registry.sqlite3 >& ingestSdqa_ImSim.log 

# Run finishDb script 
echo "${DATAREL_DIR}/bin/ingest/finishDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu ${dbuser}_PT1_2_u_${thisrun}";
${DATAREL_DIR}/bin/ingest/finishDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu ${dbuser}_PT1_2_u_${thisrun} >& finishDb.log 



