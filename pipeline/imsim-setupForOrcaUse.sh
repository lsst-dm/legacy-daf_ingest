#!/bin/sh
source /lsst/DC3/stacks/default/loadLSST.sh
RIGHTHERE=$PWD
setup ctrl_orca
cd $CTRL_ORCA_DIR
setup -r .
cd /home/rallsman/datarel
setup -r .
setup obs_lsstSim
setup astrometry_net_data imsim_star_gal
cd $RIGHTHERE
