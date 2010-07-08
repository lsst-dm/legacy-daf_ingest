#!/usr/bin/env python
#

from optparse import OptionParser
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.obs.cfht import CfhtMapper

usage = "usage: %prog  option  VISIT\nwhere\n   VISIT - identifies the visit selected\nExample: %prog --cfht 793310\nExample: %prog --imsim 85471048"
parser = OptionParser( usage = usage )
parser.add_option("-i", "--imsim", action="store_true", default=False, \
                  help="extract VISIT from imsim dataset")
parser.add_option("-c", "--cfht", action="store_true", default=False,\
                  help="extract VISIT from CFHT dataset")

(options, args) = parser.parse_args()

if len(args) != 1:
    parser.error("provide a single VISIT")
visitSelected = args[0]

if options.imsim and options.cfht:
    parser.error("options --imsim and --cfht are mutually exclusive")

if options.imsim:
   imsimRoot="/lsst/DC3/data/obs/ImSim"
   bf = dafPersist.ButlerFactory( mapper=LsstSimMapper( root=imsimRoot ))
   butler = bf.create()
   print ">intids visit snap"
   for visit, raft, sensor, snap, channel in \
           butler.queryMetadata("raw", "channel", \
                ("visit", "raft", "sensor", "snap", "channel"), visit=visitSelected):
       print "raw visit=%d snap=%d raft=%s sensor=%s channel=%s" \
                 % ( visit,  snap, raft, sensor, channel )
else:
   cfhtRoot="/lsst/DC3/data/obstest/CFHTLS/"
   bf = dafPersist.ButlerFactory( mapper=CfhtMapper( root=cfhtRoot ))
   butler = bf.create()
   print ">intids visit ccd amp"
   for visit, ccd, amp in \
           butler.queryMetadata( "raw", "ccd", \
               ( "visit", "ccd", "amp" ), \
               visit=visitSelected ):
        print "raw visit=%d  ccd=%s amp=%s"  \
                 % ( visit,  ccd, amp )

   
