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
    echo "Usage: $0 [-debug] <branch>"
    echo "Bootstrap and then process a Weekly Production Run."
    echo
    echo "Parameters (must be in this order):"
    echo "      -debug: use limited raft set for debug run."
    echo "    <branch>: one of"
    echo "              'tags'  - use stack comprised of current tags;"
    echo "              'trunk' - use stack comprised of trunk versions."
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
    echo "$PROG : Using the debug miminal-raft setup."
    shift 1
else
    DEBUG_DATA=1
fi

if [ "$1" = "tags" -o "$1" = "trunk" ] ; then
    echo "$PROG: Running for stack: $1 "
else
    usage
    exit 1
fi


# grab the date for labelling the run
i=`date "+%Y_%m%d"`

# stackType="trunk" stackType="tags"
stackType=$1

if [ "$stackType" = "tags" ] ; then

    export SHELL=/bin/bash
    export LSST_HOME=/lsst/DC3/stacks/default
    source /lsst/DC3/stacks/default/loadLSST.sh

    # following: undo gratuitous set of svn+ssh  for all lsst users
    export SVNROOT=svn://svn.lsstcorp.org
    export LSST_SVN=svn://svn.lsstcorp.org
    export LSST_DMS=svn://svn.lsstcorp.org/DMS

    setup datarel

else

    export SHELL=/bin/bash
    export LSST_DEVEL=/lsst/home/buildbot/buildbotSandbox
    source /lsst/DC3/stacks/default/loadLSST.sh

    # following: undo gratuitous set of svn+ssh  for all lsst users
    export SVNROOT=svn://svn.lsstcorp.org
    export LSST_SVN=svn://svn.lsstcorp.org
    export LSST_DMS=svn://svn.lsstcorp.org/DMS

    setup datarel

fi

printenv | grep SVN

eups list | grep Setup

echo DATAREL_DIR $DATAREL_DIR

# ls $DATAREL_DIR/pipeline

echo "copy pipeline directory from DATAREL"
cp -r $DATAREL_DIR/pipeline .

echo "change to the pipeline directory"
cd pipeline 

echo "copy weekly production files"
cp Weekly/* .

if [ $DEBUG_DATA = 0 ]; then
    cp  Weekly/debug_weekly.input weekly.input
fi

echo "copy PT1Pipe policy"
cp PT1Pipe/main-ImSim.paf . 

echo "launch weekly production"
nohup ./run_weekly_production.sh ${stackType}  >& weekly_production_$i.log

