#!/bin/bash
#
# Copy this file and make any needed changes.
#
export LSST_DEVEL=/u/ac/rallsman/lsstSandbox
source /cfs/projects/lsst/DC3/stacks/default/loadLSST.sh
setup datarel
# Only for CFHTLS run
setup obs_cfht 
setup astrometry_net_data cfhtlsDeep
#
# If you must use a private version of a module, after default setups, use:
#    setup -j <eups_name> <eups_version>
#
eups list --setup
