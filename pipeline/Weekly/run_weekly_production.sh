#!/bin/sh

# This script     run_weekly_production.sh   
# uses files
#          load_env_trunk.sh  or  load_env.tags.sh
#          stack_trunk.sh     or  stack_tags.sh
#          weekly_production.paf
#
# from the current working directory 

# grab the date for labelling the run
i=`date "+%Y_%m%d"`
echo $i

# Output directory space for the run:
# Production will make a directory 'thisrun' under base.
# 
base="/lsst3/weekly"

stackType="tags"
# stackType="trunk"

# thisrun="variability_trunk_prod_$i"
thisrun="test_${stackType}_prod1_$i"

source ./load_env_${stackType}.sh

# check versions being used
which orca.py
which eups

echo PWD 
echo $PWD 

echo RUNNING 

cd /lsst3/weekly/pipeline

# Runid for the weekly production 
# thisrun="weekly_trunk_prod_$i"

# echo "orca.py -r $PWD -e $PWD/stack_trunk.sh -V 30 -L 2 production.paf ${thisrun}"
echo "orca.py -r $PWD -e $PWD/stack_${stackType}.sh -V 30 -L 2 weekly_production.paf ${thisrun}"
sleep 5

# orca.py -r $PWD -e $PWD/stack_trunk.sh -V 30 -L 2 production.paf ${thisrun} >& unifiedPipeline.log
orca.py -r $PWD -e $PWD/stack_${stackType}.sh -V 30 -L 2 weekly_production.paf ${thisrun} >& unifiedPipeline.log

# cd ${base}/datarel-runs/weeklytest_$i
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






