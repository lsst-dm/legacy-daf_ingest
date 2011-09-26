#!/bin/sh
export LSST_HOME=/lsst/DC3/stacks/default
export SHELL=/bin/bash
source $LSST_HOME/loadLSST.sh


setup datarel

eups list | grep Setup


