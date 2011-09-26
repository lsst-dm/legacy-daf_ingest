#!/bin/sh

# This script     run_weekly_production.sh   
# uses files
#          load_env_trunk.sh  or  load_env.tags.sh
#          stack_trunk.sh     or  stack_tags.sh
#          weekly_production.paf
#
# from the current working directory 


# Initial directory on this script's startup
startDir=$PWD


if [ $# -eq 0 ]; then
    echo "$0 : You must supply 'trunk' or 'tags' as a command line argument"
    exit 1
fi


if [ $1 = 'tags' -o $1 = 'trunk' ]; then
    echo "Running for stack: $1 "
else
    echo "$1 : You must supply 'trunk' or 'tags' as a command line argument"
    exit 1
fi

echo "Starting run_weekly_production: $0"

# grab the date for labelling the run
i=`date "+%Y_%m%d_%H%M%S"`
echo $i

# Output directory space for the run:
# Production will make a directory 'thisrun' under base.
# 
base="/lsst3/weekly/datarel-runs"
dbuser=$USER

# stackType="tags" stackType="trunk"
stackType=$1
echo "stackType ${stackType}"


# Runid for the weekly production 
thisrun="wp_${stackType}_$i"

echo "startDir $startDir"
echo "------------------------------------------------------------------"
echo "Env init file:"
cat ${startDir}/stack_${stackType}.sh
echo "------------------------------------------------------------------"
echo " weekly_production.paf"
cat weekly_production.paf
echo "------------------------------------------------------------------"
echo "weekly.input"
cat weekly.input
echo "------------------------------------------------------------------"
echo "runID ${thisrun}"
echo "FullPathToWeeklyRun: ${base}/${thisrun}"
echo "startDir $startDir"

echo RUNNING 
echo "orca.py -r $startDir -e ${startDir}/stack_${stackType}.sh -V 30 -L 2 weekly_production.paf ${thisrun}"

orca.py -r $startDir -e ${startDir}/stack_${stackType}.sh -V 30 -L 2 weekly_production.paf ${thisrun} >& unifiedPipeline.log
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in orca pipeline execution."
     exit 1
fi


# Update config/weekly.tags
echo "Saving configuration in: ${base}/${thisrun}/config/weekly.tags"
mkdir -p ${base}/${thisrun}/config
echo "cp ${base}/${thisrun}/work/PT1PipeA_1/eups-env.txt ${base}/${thisrun}/config/weekly.tags"
cp ${base}/${thisrun}/work/PT1PipeA_1/eups-env.txt ${base}/${thisrun}/config/weekly.tags
cat ${base}/${thisrun}/config/weekly.tags
echo " ---- D I F F E R E N C E S   between pipelines' setups    B E L O W ----"
for f in ${base}/${thisrun}/work/PT1Pipe*/eups-env.txt; do
    diff ${base}/${thisrun}/config/weekly.tags $f
done
echo " ----  A N Y    D I F F E R EN C E S    N O T E D    A B O V E   ?   ----"

cd ${base}/${thisrun}
pwd
# Run SourceAssoc
mkdir SourceAssoc
echo "${DATAREL_DIR}/bin/sst/SourceAssoc_ImSim.py -i update -o SourceAssoc -R update/registry.sqlite3";
${DATAREL_DIR}/bin/sst/SourceAssoc_ImSim.py -i update -o SourceAssoc -R update/registry.sqlite3 >& SourceAssoc_ImSim.log 
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in SourceAssoc_ImSim execution."
     exit 1
fi

# Prepare DB
echo "${DATAREL_DIR}/bin/ingest/prepareDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu ${dbuser}_PT1_2_u_${thisrun}";
${DATAREL_DIR}/bin/ingest/prepareDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu ${dbuser}_PT1_2_u_${thisrun} >& prepareDb.log 
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in prepareDb execution."
     exit 1
fi


# Ingest processed metadata
echo "${DATAREL_DIR}/bin/ingest/ingestProcessed_ImSim.py -u ${dbuser} -d ${dbuser}_PT1_2_u_${thisrun} update update/registry.sqlite3";
${DATAREL_DIR}/bin/ingest/ingestProcessed_ImSim.py -u ${dbuser} -d ${dbuser}_PT1_2_u_${thisrun} update update/registry.sqlite3 >& ingestProcessed_ImSim.log
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in ingestProcessed_Imsim execution."
     exit 1
fi

# Ingest source association data
mkdir csv-SourceAssoc
echo "${DATAREL_DIR}/bin/ingest/ingestSourceAssoc.py -m -u ${dbuser} -R input/refObject.csv  -e Science_Ccd_Exposure_Metadata.csv -H lsst10.ncsa.uiuc.edu -j 1 ${dbuser}_PT1_2_u_${thisrun}  SourceAssoc  csv-SourceAssoc";
${DATAREL_DIR}/bin/ingest/ingestSourceAssoc.py -m -u ${dbuser} -R input/refObject.csv -e Science_Ccd_Exposure_Metadata.csv -H lsst10.ncsa.uiuc.edu -j 1 ${dbuser}_PT1_2_u_${thisrun}  SourceAssoc  csv-SourceAssoc >& ingestSourceAssoc.log 
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in ingestSourceAssoc execution."
     exit 1
fi

# Run SQDA ingestion script 
echo "${DATAREL_DIR}/bin/ingest/ingestSdqa_ImSim.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -d ${dbuser}_PT1_2_u_${thisrun}  update update/registry.sqlite3 ";
${DATAREL_DIR}/bin/ingest/ingestSdqa_ImSim.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -d ${dbuser}_PT1_2_u_${thisrun} update  update/registry.sqlite3 >& ingestSdqa_ImSim.log 
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in ingestSdqa_ImSim execution."
     exit 1
fi

# Run finishDb script 
#   N O T E      The -t (transpose) option takes an v. long time to process. 
#   N O T E      Do NOT use for full production run.  
echo "${DATAREL_DIR}/bin/ingest/finishDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -t ${dbuser}_PT1_2_u_${thisrun}";
${DATAREL_DIR}/bin/ingest/finishDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -t ${dbuser}_PT1_2_u_${thisrun} >& finishDb.log 
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in finishDB execution."
     exit 1
fi

#-------------------------------------------------------------------------
# Update DB: 'buildbot_weekly_latest' with the details about this weekly run
echo "${DATAREL_DIR}/bin/ingest/linkDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -t ${stackType} ${dbuser}_PT1_2_u_${thisrun}";
${DATAREL_DIR}/bin/ingest/linkDb.py -u ${dbuser} -H lsst10.ncsa.uiuc.edu -t ${stackType} ${dbuser}_PT1_2_u_${thisrun} >& linkDb.log 
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in linkDb execution."
     exit 1
fi

# Only continue processing  if NOT Debug mode.
# Having to approximate by checking if input list is very short
if [ `cat ${startDir}/weekly.input | wc -l` -le 21 ] ; then
    echo "Not altering latest_${stackType} since DEBUG mode."
    exit 0
fi


# Update sym link: latest_<type>
#     Don't change the rundir latest_* link if this is One-Off
#     Don't want to bundle both sym link sets 'cuz pipeQA might fail
NOT_ONEOFF=`cat ${startDir}/weekly_production.paf | grep OneOff | grep -v '.*#.*dataRepository:' | grep 'dataRepository:' | wc -l `
echo "Not ONEOFF=${NOT_ONEOFF}"
if [ ${NOT_ONEOFF} -eq 0  ] ; then
    rm -f ${base}/latest_${stackType}
    echo "ln -s ${base}/${thisrun} ${base}/latest_${stackType}"
    ln -s ${base}/${thisrun} ${base}/latest_${stackType}
fi

# Initiate pipeQA
setup testing_pipeQA
setup testing_displayQA
export WWW_ROOT=/lsst/public_html/pipeQA/html/dev
export WWW_RERUN="${dbuser}_PT1_2_u_${thisrun}"
newQa.py ${WWW_RERUN}
${TESTING_PIPEQA_DIR}/bin/pipeQa.py -d -f -k -b ccd -v ".*" ${WWW_RERUN}

# Don't change the pipeQA latest_* link if this is a One-Off
if [ $NOT_ONEOFF -eq 0  ] ; then
    ln -sf ${WWW_ROOT}/${WWW_RERUN} ${WWW_ROOT}/latest_${stackType}
fi
