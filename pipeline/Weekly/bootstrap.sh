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
    echo "Usage: $0 [-debug] -input <weekly> <branch>"
    echo "Bootstrap and then process a Weekly Production Run."
    echo ""
    echo "Parameters (must be in this order):"
    echo "          -debug: use limited raft set for debug run."
    echo "  input <weekly>: selects file specifying image data to process. "
    echo "                  Located in $DATAREL_DIR/pipeline/Weekly/<weekly>."
    echo "        <branch>: one of"
    echo "                 'tags'  - use stack comprised of current tags;"
    echo "                 'trunk' - use stack comprised of trunk versions."
    echo ""
}
#--------------------------------------------------------------------------


PROG=$0

if [ $# -eq 0 ] ; then
    usage
    exit 1
fi

if [ "$1" = "-debug" ] ; then
    DEBUG_DATA=0
    echo "$PROG : Using debug miminal-raft setup."
    shift 1
else
    DEBUG_DATA=1
fi

if [ $# -lt 3 ] ; then
    usage
    exit 1
fi

if [ "$1" = "-input" ] ; then
    INPUT_DATA=$2
    echo "$PROG : Processing input data: $INPUT_DATA."
    shift 2
fi

if [ "$1" = "tags" -o "$1" = "trunk" ] ; then
    echo "$PROG : Running for stack: $1 "
else
    usage
    exit 1
fi


# grab the date for labelling the run
i=`date "+%Y_%m%d"`

# stackType="trunk" stackType="tags"
stackType=$1

# Setup Run Environment to reflect Stack (Trunk vs Tagged) being used
# Can't use the nice scripts (pipeline/Weekly/{stack_trunk.sh stack_tags.sh})
#     since don't have appropriate copy of datarel available yet.
# If you change this block, you need to update the two scripts named above.
export SHELL=/bin/bash
export LSST_HOME=/lsst/DC3/stacks/default
if [ "$stackType" = "trunk" ] ; then
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
# End: Setup Run Environment

printenv | grep SVN
eups list -s
echo DATAREL_DIR $DATAREL_DIR


echo "copy pipeline directory from DATAREL"
cp -r $DATAREL_DIR/pipeline .

echo "change to the pipeline directory"
cd pipeline 

echo "copy weekly production files"
cp Weekly/* .

if [ $DEBUG_DATA = 0 ]; then
    head -1  Weekly/$INPUT_DATA > weekly.input
    tail -20  Weekly/$INPUT_DATA >> weekly.input
else
    cp Weekly/$INPUT_DATA weekly.input
fi

echo "copy PT1Pipe policy"
cp PT1Pipe/main-ImSim.paf . 

echo "launch weekly production"
nohup ./run_weekly_production.sh ${stackType}  >& weekly_production_$i.log

# A little handshake twix scripts to get name of Weekly Run Result Directory
WeeklyRunDir=`grep "FullPathToWeeklyRun: "  weekly_production_$i.log | sed -e "s/FullPathToWeeklyRun: //"`

base=`dirname ${WeeklyRunDir}`
echo "RootDir: ${base}"
if [ $DEBUG_DATA = 1 ]; then
    # only switch sym link to latest production run if NOT Debug mode.
    rm -f ${base}/latest_${stackType}
    ln -s ${WeeklyRunDir} ${base}/latest_${stackType}
    echo "Relinking ${base}/latest_${stackType} to: ${WeeklyRunDir}"
fi
echo "Move log into Weekly Run output archive: ${WeeklyRunDir}"
cp weekly_production_$i.log ${WeeklyRunDir}/

