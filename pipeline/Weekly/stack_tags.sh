#!/bin/sh
# Setup Run Environment to reflect Tagged Stack  use

export SHELL=/bin/bash

CURRENT=$PWD

export SHELL=/bin/bash
export LSST_HOME=/lsst/DC3/stacks/default
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

cd $CURRENT 

