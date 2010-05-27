#!/bin/bash
orgDir=$PWD
source /lsst/DC3/stacks/default/loadLSST.sh
setup ctrl_orca
setup datarel
cd $DATAREL_DIR
setup -r .
setup obs_cfht
setup astrometry_net_data cfhtlsDeep
eups list --setup
cd $orgDir
