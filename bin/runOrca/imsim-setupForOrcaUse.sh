#!/bin/bash
#
# Copy this file and make any needed changes.
#
source /lsst/DC3/stacks/default/loadLSST.sh
setup ctrl_orca
setup datarel
setup obs_lsstSim
setup astrometry_net_data imsim_full
eups list --setup
