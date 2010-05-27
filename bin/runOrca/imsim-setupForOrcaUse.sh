#!/bin/sh
orgDir=$PWD
source /lsst/DC3/stacks/default/loadLSST.sh
setup ctrl_orca
setup datarel
cd $DATAREL_DIR
setup -r .
setup obs_lsstSim
#setup astrometry_net_data imsim_star_gal
setup -r ~dstn/imsim/astrometry_net_data_imsim_trunk
cd $orgDir
