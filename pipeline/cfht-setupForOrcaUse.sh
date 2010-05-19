#/bin/tcsh
source /lsst/DC3/stacks/default/loadLSST.csh
set RIGHTHERE=$PWD
setup ctrl_orca
cd $CTRL_ORCA_DIR
setup -r .
cd /home/rallsman/datarel
setup -r .
setup obs_cfht
setup astrometry_net_data cfhtlsDeep
cd $RIGHTHERE
