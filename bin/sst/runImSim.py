#!/usr/bin/env python

# setup datarel
# setup obs_lsstSim
# setup astrometry_net_data imsim_20100611

from optparse import OptionParser
import os
import sys
import traceback

from ISR_ImSim import isrProcess
from CcdAssembly_ImSim import ccdAssemblyProcess
from CrSplit_ImSim import crSplitProcess
from ImgChar_ImSim import imgCharProcess
from SFM_ImSim import sfmProcess

import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper

def process(inButler, outButler, visit, raft, sensor, force=False):
    print >>sys.stderr, "****** Processing visit %d raft %s sensor %s" % \
            (visit, raft, sensor)
    if outButler.datasetExists("src", visit=visit, raft=raft, sensor=sensor):
        return
    if force or not outButler.datasetExists("calexp",
            visit=visit, raft=raft, sensor=sensor):
        if force or not outButler.datasetExists("visitim",
                visit=visit, raft=raft, sensor=sensor):
            for snap in inButler.queryMetadata("raw", "snap"):
                if force or not outButler.datasetExists("postISRCCD",
                        visit=visit, snap=snap, raft=raft, sensor=sensor):
                    for channel in inButler.queryMetadata("raw", "channel"):
                        if force or not outButler.datasetExists("postISR",
                                visit=visit, snap=snap,
                                raft=raft, sensor=sensor, channel=channel):
                            isrProcess(inButler=inButler, outButler=outButler,
                                    visit=visit, snap=snap,
                                    raft=raft, sensor=sensor, channel=channel)
                    ccdAssemblyProcess(inButler=outButler, outButler=outButler,
                            visit=visit, snap=snap, raft=raft, sensor=sensor)
            crSplitProcess(inButler=outButler, outButler=outButler,
                    visit=visit, raft=raft, sensor=sensor)
        imgCharProcess(inButler=outButler, outButler=outButler,
                visit=visit, raft=raft, sensor=sensor)
    sfmProcess(inButler=outButler, outButler=outButler,
            visit=visit, raft=raft, sensor=sensor)

def main():
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="root",
            default="/lsst/DC3/data/obstest/ImSim", help="input root")
    parser.add_option("-o", "--output", dest="outRoot", default=".",
            help="output root")
    parser.add_option("-f", "--force", action="store_true",
            default=False,
            help="execute even if output dataset exists")
    parser.add_option("-C", "--calibRoot", dest="calibRoot",
            help="calibration root")
    parser.add_option("-R", "--registry", help="registry")
    parser.add_option("-v", "--visit", action="append", type="int",
            help="visit numbers (can be repeated)")
    parser.add_option("-r", "--raft", action="append", type="string",
            help="raft name (can be repeated)")
    parser.add_option("-s", "--sensor", action="append", type="string",
            help="sensor name (can be repeated)")
    (options, args) = parser.parse_args()

    if options.registry is None:
        if os.path.exists(os.path.join(options.root, "registry.sqlite3")):
            options.registry = os.path.join(options.root, "registry.sqlite3")
    if options.registry is None:
        if os.path.exists("/lsst/DC3/data/obstest/ImSim/registry.sqlite3"):
            options.registry = "/lsst/DC3/data/obstest/ImSim/registry.sqlite3"
    if options.calibRoot is None:
        if os.path.exists("/lsst/DC3/data/obstest/ImSim"):
            options.calibRoot = "/lsst/DC3/data/obstest/ImSim"

    bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
        root=options.root, calibRoot=options.calibRoot,
        registry=options.registry))
    inButler = bf.create()
    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
        root=options.outRoot, registry=options.registry))
    outButler = obf.create()

    if options.visit is None:
        print >>sys.stderr, "Running over all input visits"
        options.visit = inButler.queryMetadata("raw", "visit")
    elif not hasattr(options.visit, "__iter__"):
        options.visit = [options.visit]
    if options.raft is None:
        print >>sys.stderr, "Running over all rafts"
        options.raft = inButler.queryMetadata("raw", "raft")
    elif not hasattr(options.raft, "__iter__"):
        options.raft = [options.raft] 
    if options.sensor is None:
        print >>sys.stderr, "Running over all sensors"
        options.sensor = inButler.queryMetadata("raw", "sensor")
    elif not hasattr(options.sensor, "__iter__"):
        options.sensor = [options.sensor] 

    for visit in options.visit:
        for raft in options.raft:
            for sensor in options.sensor:
                try:
                    process(inButler, outButler, visit, raft, sensor,
                            options.force)
                except Exception, e:
                    traceback.print_exc()
                    print >>sys.stderr, "Continuing..."

if __name__ == "__main__":
    main()
