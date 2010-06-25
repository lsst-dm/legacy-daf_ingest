#!/bin/bash
#
# Copy this file and make any needed changes.
#
export LSST_DEVEL=/u/ac/rallsman/lsstSandbox
source /cfs/projects/lsst/DC3/stacks/default/loadLSST.sh
setup datarel
setup -j afw 3.6.0-svn15913
setup -j ap 3.2.1+svn15516
setup -j ip_isr 3.4.5+3
setup -j ip_utils 3.0.0+1
setup -j ip_pipeline  3.2.0+svn15431
setup -j meas_algorithms 3.2.0+2
setup -j meas_astrom 3.2.0+svn15916
setup -j meas_utils 3.2.0+svn15840
setup -j meas_pipeline 3.2.0+svn15431
setup -j daf_persistence 3.3.13+svn15478
setup -j obs_lsstSim 3.0.10+svn15481
setup astrometry_net_data imsim_20100611_robyn
eups list --setup
