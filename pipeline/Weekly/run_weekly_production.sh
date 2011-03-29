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
i=`date "+%Y_%m%d"`
echo $i

# Output directory space for the run:
# Production will make a directory 'thisrun' under base.
# 
base="/lsst3/weekly"

# stackType="tags" stackType="trunk"
stackType=$1
echo "stackType ${stackType}"


# Runid for the weekly production 
thisrun="weekly_production_${stackType}_$i"
echo "runID ${thisrun}"

echo "Running: source ./load_env_${stackType}.sh "
source ./load_env_${stackType}.sh

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
echo "${DATAREL_DIR}/bin/ingest/prepareDb.py -u rplante -H lsst10.ncsa.uiuc.edu rplante_DC3b_u_${thisrun}_science";
${DATAREL_DIR}/bin/ingest/prepareDb.py -u rplante -H lsst10.ncsa.uiuc.edu rplante_DC3b_u_${thisrun}_science >& prepareDb.log 

# Ingest processed metadata
echo "${DATAREL_DIR}/bin/ingest/ingestProcessed_ImSim.py -u rplante -d rplante_DC3b_u_${thisrun}_science update update/registry.sqlite3";
${DATAREL_DIR}/bin/ingest/ingestProcessed_ImSim.py -u rplante -d rplante_DC3b_u_${thisrun}_science update update/registry.sqlite3 >& ingestProcessed_ImSim.log

# Ingest source association data
mkdir csv-SourceAssoc
echo "${DATAREL_DIR}/bin/ingest/ingestSourceAssoc.py -m -u rplante -e /lsst3/weekly/datarel-runs/${thisrun}/Science_Ccd_Exposure_Metadata.csv -H lsst10.ncsa.uiuc.edu -j 1 rplante_DC3b_u_${thisrun}_science  SourceAssoc  csv-SourceAssoc";
${DATAREL_DIR}/bin/ingest/ingestSourceAssoc.py -m -u rplante -e /lsst3/weekly/datarel-runs/${thisrun}/Science_Ccd_Exposure_Metadata.csv -H lsst10.ncsa.uiuc.edu -j 1 rplante_DC3b_u_${thisrun}_science  SourceAssoc  csv-SourceAssoc >& ingestSourceAssoc.log 

# Run SQDA igestion script 
echo "${DATAREL_DIR}/bin/ingest/ingestSdqa_ImSim.py -u rplante -H lsst10.ncsa.uiuc.edu -d rplante_DC3b_u_${thisrun}_science  update update/registry.sqlite3 ";
${DATAREL_DIR}/bin/ingest/ingestSdqa_ImSim.py -u rplante -H lsst10.ncsa.uiuc.edu -d rplante_DC3b_u_${thisrun}_science update  update/registry.sqlite3 >& ingestSdqa_ImSim.log 

# Run finishDb script 
echo "${DATAREL_DIR}/bin/ingest/finishDb.py -u rplante -H lsst10.ncsa.uiuc.edu -d rplante_DC3b_u_${thisrun}_science";
${DATAREL_DIR}/bin/ingest/finishDb.py -u rplante -H lsst10.ncsa.uiuc.edu -d rplante_DC3b_u_${thisrun}_science >& finishDb.log 




