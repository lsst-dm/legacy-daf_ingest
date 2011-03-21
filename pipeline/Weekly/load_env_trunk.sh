#!/bin/sh

export SHELL=/bin/bash
# export LSST_DEVEL=/lsst/home/buildbot/buildbotSandbox 
export LSST_DEVEL=/home/buildbot/buildbotSandbox 
source /lsst/DC3/stacks/default/loadLSST.sh 


setup datarel  

eups list | grep Setup

