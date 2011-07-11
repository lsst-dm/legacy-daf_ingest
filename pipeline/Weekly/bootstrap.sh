#!/bin/bash
#
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

#--------------------------------------------------------------------------
usage() {
#80 cols  ................................................................................
    echo ""
    echo "Usage: $0 [-debug] [-dataRepository <pathname>] -input <list>  [-overlay <pathname>] <branch>"
    echo "Bootstrap and then process a Weekly Production Run."
    echo ""
    echo "Parameters (must be in this order):"
    echo " -debug:          use limited raft set for debug run."
    echo " -dataRepository: fullpath to input data repository. Default uses definition in weekly_production.paf"
    echo " -input <list>: selects file specifying image data to process."
    echo "                Either: full pathname of file containing input list;"
    echo "                or: pre-defined file within $DATAREL_DIR/pipeline/Weekly/"
    echo " -overlay <pathname>:  pathname of eups-setup script to overlay selected <branch> packages with different versions ."
    echo " <branch>:        one of"
    echo "                  'tags'  - load  stack comprised of current tags;"
    echo "                  'trunk' - load  stack comprised of trunk versions."
    echo ""
}
#--------------------------------------------------------------------------


PROG=$0

if [ $# -eq 0 ] ; then
    usage
    exit 1
fi

echo $1
if [ "$1" = "-debug" ] ; then
    DEBUG_DATA=0
    echo "$PROG : Using debug miminal-raft setup."
    shift 1
else
    DEBUG_DATA=1
fi

echo $1
if [ "$1" = "-dataRepository" ] ; then
    if [ -e $2 ] ; then
        DATA_REPOSITORY=$2
        echo "$PROG : Using dataRepository: $DATA_REPOSITORY."
        shift 2
    else
        echo "$PROG : Data repository: $2, does not exist."
        exit 1
    fi
fi

echo $1
if [ "$1" != "-input" ] ; then
    usage
    exit 1
elif [ -e $2 ] ; then
    INPUT_LIST=$2
    echo "$PROG : Processing local_input data: $INPUT_LIST."
    shift 2
else 
    INPUT_LIST="$2"
    echo "$PROG : Processing input data: $INPUT_LIST."
    shift 2
fi

echo $1
unset OVERLAY
if [ "$1" = "-overlay" ] ; then
    if  [ ! -e "$2" ]; then
        echo "$PROG : Overlay file, $2, does not exist"
        usage 
        exit 1
    fi
    OVERLAY=$2
    echo "$PROG : Customizing the base stack from: $OVERLAY"
    shift 2
fi


if [ "$1" = "tags" -o "$1" = "trunk" ] ; then
    STACK_TYPE=$1
    echo "$PROG : Running for stack: $1 "
    shift 1
else
    usage
    exit 1
fi


# grab the date for labelling the run
i=`date "+%Y_%m%d"`


# Setup Run Environment to reflect Stack (Trunk vs Tagged) being used
# Can't use the nice scripts (pipeline/Weekly/{stack_trunk.sh stack_tags.sh})
#     since don't have appropriate copy of datarel available yet.
# If you change this block, you need to update the two scripts named above.
export SHELL=/bin/bash
export LSST_HOME=/lsst/DC3/stacks/default
if [ "$STACK_TYPE" = "trunk" ] ; then
    export LSST_DEVEL=/lsst/home/buildbot/buildbotSandbox
fi
source /lsst/DC3/stacks/default/loadLSST.sh
# following: undo gratuitous set of svn+ssh  for all lsst users
export SVNROOT=svn://svn.lsstcorp.org
export LSST_SVN=svn://svn.lsstcorp.org
export LSST_DMS=svn://svn.lsstcorp.org/DMS
setup datarel
# Following required and AFTER datarel setup since the tagged production run
#    actually overrides the 'current' and sets up 'cfhttemplate'.
#    May ultimately need to add new param to script to designate desired
#    astrometry_net_data for the run.
setup astrometry_net_data

# Now overlay with any customization of the package setup
if [ "$OVERLAY" != "" ] ; then
   if [ -e $OVERLAY ] ; then
      source $OVERLAY
   else
      echo "$PROG : Overlay file: $OVERLAY, does not exist."
      exit 1
   fi
fi
# End: Setup Run Environment

printenv | grep SVN
eups list -s

cp -r $DATAREL_DIR/pipeline pipeline
cp pipeline/Weekly/* pipeline

# Now its time to customize the various defaults being overriden:
# Update pipelines' env initscript:stack_*.sh, with $OVERLAY 
if [ "$OVERLAY" != "" ]; then
    cat $OVERLAY >> pipeline/stack_${STACK_TYPE}.sh
    echo "Modified pipeline/stack_${STACK_TYPE}.sh"
    cat pipeline/stack_${STACK_TYPE}.sh
    echo "---------------------"
fi

# Create weekly.input with $INPUT_LIST
if [ "$DEBUG_DATA" = "0" ]; then
    head -1  $INPUT_LIST > pipeline/weekly.input
    tail -20  $INPUT_LIST >> pipeline/weekly.input
else
    cp $INPUT_LIST pipeline/weekly.input
fi


echo "change to pipeline directory"
cd pipeline 

#    update weekly_production.paf with customized $DATA_REPOSITORY
if [ "$DATA_REPOSITORY" != "" ] ; then
    cp weekly_production.paf weekly_production.paf.bak
    cat weekly_production.paf.bak | sed -e "s^dataRepository:.*^dataRepository:$DATA_REPOSITORY^" > weekly_production.paf
fi
echo "weekly_production.paf"
cat  weekly_production.paf
echo "---------------------"

echo "position PT1Pipe policy into pipeline/"
cp PT1Pipe/main-ImSim.paf . 

#__________________________________________________________________________
#       Use this block For transient mods to datarel policy files
#       Modify as needed to replace policy parameter in correct file.
#
#       For most trunk stacks, just update datarel svn then rebuild trunk.
#__________________________________________________________________________
if [ "$STACK_TYPE" = "tags" ] ; then
    cp PT1Pipe/SFM-sourceMeasure.paf PT1Pipe/SFM-sourceMeasure.paf_bak
    cat PT1Pipe/SFM-sourceMeasure.paf_bak | sed -e "s^enabled: true^enabled: false^"> PT1Pipe/SFM-sourceMeasure.paf
    echo "Temporarily modified for TAGS use: PT1Pipe/SFM-sourceMeasure.paf"
    cat PT1Pipe/SFM-sourceMeasure.paf
    echo "---------------------"
fi
#__________________________________________________________________________
#__________________________________________________________________________

echo "launch weekly production"
echo "./run_weekly_production.sh ${STACK_TYPE}  >& weekly_production_$i.log"
nohup ./run_weekly_production.sh ${STACK_TYPE}  >& weekly_production_$i.log
if [ $? -ne 0 ]; then
     echo "------------------------------------"
     echo "FATAL: Failed in run_weekly_production execution."
     exit 1
fi

