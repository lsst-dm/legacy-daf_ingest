#!/bin/sh

export SHELL=/bin/bash

export LSST_HOME=/lsst/DC3/stacks/default/loadLSST.sh 
export LSST_DEVEL=/lsst/home/$USER/buildbotSandbox 

source $LSST_HOME/loadLSST.sh 

setup datarel  

eups list | grep Setup

