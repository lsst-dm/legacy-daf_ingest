#!/bin/bash
#
# Copy this file and make any needed changes.
#
#export LSST_DEVEL=/u/ac/rallsman/lsstSandbox
source /cfs/projects/lsst/DC3/stacks/default/loadLSST.sh
#-----------------------------------------------------
# Uncomment following entry depending on dataset 
#-----------------------------------------------------

setup -j astrometry_net_data   cfhtls_20100706
# setup -j astrometry_net_data   imsim_20100625

#-----------------------------------------------------
# PT1 Production Software Suite
#-----------------------------------------------------
setup -j afw                   3.6.0          
setup -j ap                    3.2.2+1        
setup -j astrometry_net        0.30           
setup -j base                  3.1.2          
setup -j cat                   3.4.2          
setup -j coadd_utils           3.0.1+4        
setup -j ctrl_events           3.11.8
setup -j ctrl_orca             3.7.3          
setup -j ctrl_provenance       3.1            
setup -j ctrl_sched            3.0.4          
setup -j daf_base              3.2.14         
setup -j daf_butlerUtils       3.0.4          
setup -j daf_data              3.2.4
setup -j daf_persistence       3.3.14         
setup -j datarel               3.0.1          
setup -j geom                  3.0.4          
setup -j ip_diffim             3.4.0+5        
setup -j ip_isr                3.4.6          
setup -j ip_pipeline           3.0.5          
setup -j ip_utils              3.0.0+1        
setup -j lsst                  1.0            
setup -j lssteups              1.0            
setup -j meas_algorithms       3.2.0+1        
setup -j meas_astrom           3.2.1          
setup -j meas_pipeline         3.2.2          
setup -j meas_utils            3.2.2          
setup -j mops                  3.2.5+3        
setup -j obs_cfht              3.0.9          
setup -j obs_lsstSim           3.0.12         
setup -j pex_exceptions        3.2.2          
setup -j pex_harness           3.6.10         
setup -j pex_logging           3.4.1          
setup -j pex_policy            3.5.2          
setup -j scons                 3.3            
setup -j sconsDistrib          0.98.5         
setup -j sconsUtils            3.3            
setup -j sdqa                  3.0.12         
setup -j security              3.2.2          
setup -j skypix                3.0.3          
setup -j utils                 3.4.5          
#-----------------------------------------------------
# PT1 Third Party Software
#-----------------------------------------------------
setup -j activemqcpp           3.1.2          
setup -j apr                   1.3.3          
setup -j boost                 1.37.0         
setup -j cfitsio               3006.2         
setup -j condor_glidein        7.4.2          
setup -j doxygen               1.5.9          
setup -j eigen                 2.0.0          
setup -j eups                  1.1.1  
setup -j fftw                  3.1.2+1        
setup -j gcc                   4.3.3          
setup -j gsl                   1.8            
setup -j minuit2               5.22.00+1      
setup -j mpich2                1.0.5p4+1      
setup -j mysqlclient           5.0.45+1       
setup -j mysqlpython           1.2.2          
setup -j numpy                 1.2.1          
setup -j pyfits                2.2.2          
setup -j pysqlite              2.6.0          
setup -j python                2.5.2          
setup -j sqlite                3.6.23.1       
setup -j swig                  1.3.36+2       
setup -j tcltk                 8.5a4          
setup -j wcslib                4.4.4          
setup -j xpa                   2.1.7b2        
setup -j zlib                  1.2.5          

#
eups list --setup -v
