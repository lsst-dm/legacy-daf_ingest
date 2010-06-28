#!/bin/bash
#
# Copy this file and make any needed changes.
#
export LSST_DEVEL=/u/ac/rallsman/lsstSandbox
source /cfs/projects/lsst/DC3/stacks/default/loadLSST.sh
setup datarel
setup -j afw afw_3.6.0
setup -j ap 3.2.1+svn15516
setup -j ip_isr 3.4.5+3
setup -j ip_utils 3.0.0+1
setup -j ip_pipeline  3.2.0+svn15431
setup -j meas_algorithms meas_algorithms_3.2.0+2
setup -j meas_astrom meas_astrom_3.2.0+svn15995
setup -j meas_utils meas_utils_3.2.1
setup -j meas_pipeline meas_pipeline_3.2.0+svn15995
setup -j daf_persistence daf_persistence_3.3.14
setup -j pex_harness pex_harness_3.6.10
setup -j pex_policy pex_policy_3.5.2
setup -j ctrl_provenance 3.1
# only for ImSim run
setup -j obs_lsstSim 3.0.10+svn15481
setup astrometry_net_data imsim_20100611_robyn
#
eups list --setup
