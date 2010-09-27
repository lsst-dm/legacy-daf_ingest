#!/bin/bash
#
# Copy this file and make any needed changes.
#
source ~/lsst-dev/DMS/loadLSST.sh
setup ctrl_orca
setup -r ~/lsst/DMS/datarel
setup -r ~/lsst/DMS/meas/astrom
setup astrometry_net_data imsim_20100716
eups list --setup
