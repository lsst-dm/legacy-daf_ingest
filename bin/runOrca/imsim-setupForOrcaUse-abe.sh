#!/bin/bash
#
# Copy this file and make any needed changes.
#
source /cfs/projects/lsst/DC3/stacks/default/loadLSST.sh
setup ctrl_orca
setup datarel
setup obs_lsstSim
#setup astrometry_net_data imsim_full
setup astrometry_net_data imsim_20100611_robyn
eups list --setup
