#!/bin/sh
export LSST_HOME=/lsst/DC3/stacks/default
export SHELL=/bin/bash
source /lsst/DC3/stacks/default/loadLSST.sh


setup datarel

eups list | grep Setup


