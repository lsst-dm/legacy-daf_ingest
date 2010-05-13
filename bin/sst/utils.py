from optparse import OptionParser
import os
import sys

import lsst.pex.policy as pexPolicy
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def runStage(stage, policyString, clip):
    if policyString.startswith("#<?cfg "):
        policyString = pexPolicy.PolicyString(policyString)
    pol = pexPolicy.Policy.createPolicy(policyString)
    sst = SimpleStageTester(stage(pol))
    return sst.runWorker(clip)

def cfhtMain(processFunction, outDatasetType, need=(), defaultRoot="."):
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="root",
            default=defaultRoot, help="input root")
    parser.add_option("-o", "--output", dest="outRoot", default=".",
            help="output root")
    if "calib" in need:
        parser.add_option("-C", "--calibRoot", dest="calibRoot",
                default="/lsst/DC3/data/obstest/CFHTLS/calib",
                help="calibration root")
    parser.add_option("-r", "--registry", help="registry")
    parser.add_option("-v", "--visit", type="int", help="visit number")
    if "ccd" in need or "amp" in need:
        parser.add_option("-c", "--ccd", type="int", help="ccd number")
    if "amp" in need:
        parser.add_option("-a", "--amp", type="int", help="amp number")
    (options, args) = parser.parse_args()

    if options.registry is None:
        if os.path.exists(os.path.join(options.root, "registry.sqlite3")):
            options.registry = os.path.join(options.root, "registry.sqlite3")
    if "calib" in need:
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(
            root=options.root, calibRoot=options.calibRoot,
            registry=options.registry))
    else:
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(
            root=options.root, registry=options.registry))
    inButler = bf.create()
    obf = dafPersist.ButlerFactory(mapper=CfhtMapper(
        root=options.outRoot, registry=options.registry))
    outButler = obf.create()

    if options.visit is None:
        print >>sys.stderr, "Running over all input visits"
        options.visit = [x[0] for x in
                inButler.queryMetadata("raw", "visit", ("visit",))]
    elif not hasattr(options.visit, "__iter__"):
        options.visit = [options.visit]
    if "ccd" in need or "amp" in need:
        if options.ccd is None:
            print >>sys.stderr, "Running over all CCDs"
            options.ccd = [x[0] for x in
                    inButler.queryMetadata("raw", "ccd", ("ccd",))]
        elif not hasattr(options.ccd, "__iter__"):
            options.ccd = [options.ccd]
    if "amp" in need:
        if options.amp is None:
            print >>sys.stderr, "Running over all amps"
            options.amp = [x[0] for x in
                    inButler.queryMetadata("raw", "amp", ("amp",))]
        elif not hasattr(options.amp, "__iter__"):
            options.amp = [options.amp]

    for visit in options.visit:
        if "ccd" in need or "amp" in need:
            for ccd in options.ccd:
                if "amp" in need:
                    for amp in options.amp:
                        if not outButler.fileExists(outDatasetType,
                                visit=visit, ccd=ccd, amp=amp):
                            print >>sys.stderr, \
                                    "***** Processing visit %d ccd %d amp %d" % \
                                    (visit, ccd, amp)
                            processFunction(inButler=inButler,
                                    outButler=outButler,
                                    visit=visit, ccd=ccd, amp=amp)
                else:
                    if not outButler.fileExists(outDatasetType,
                            visit=visit, ccd=ccd):
                        print >>sys.stderr, \
                                "***** Processing visit %d ccd %d" % \
                                (visit, ccd)
                        processFunction(inButler=inButler, outButler=outButler,
                                visit=visit, ccd=ccd)
        else:
            if not outButler.fileExists(outDatasetType, visit=visit):
                print >>sys.stderr, "***** Processing visit %d" % (visit,)
                processFunction(inButler=inButler, outButler=outButler,
                        visit=visit)
            

def lsstSimMain(processFunction, outDatasetType, need=(), defaultRoot="."):
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="root",
            default=defaultRoot, help="input root")
    parser.add_option("-o", "--output", dest="outRoot", default=".",
            help="output root")
    if "calib" in need:
        parser.add_option("-C", "--calibRoot", dest="calibRoot",
                default="/lsst/DC3/data/obstest/ImSim",
                help="calibration root")
    parser.add_option("-r", "--registry", help="registry")
    parser.add_option("-v", "--visit", type="int", help="visit number")
    parser.add_option("-r", "--raft", help="raft coords")
    parser.add_option("-c", "--sensor", help="sensor coords")
    parser.add_option("-a", "--channel", help="channel coords")
    (options, args) = parser.parse_args()

    if options.registry is None:
        if os.path.exists(os.path.join(options.root, "registry.sqlite3")):
            options.registry = os.path.join(options.root, "registry.sqlite3")
    if "calib" in need:
        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
            root=options.root, calibRoot=options.calibRoot,
            registry=options.registry))
    else:
        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
            root=options.root, registry=options.registry))
    inButler = bf.create()
    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
        root=options.outRoot, registry=options.registry))
    outButler = obf.create()

    if options.visit is None:
        print >>sys.stderr, "Running over all input visits"
        options.visit = [x[0] for x in
                inButler.queryMetadata("raw", "visit", ("visit",))]
    elif not hasattr(options.visit, "__iter__"):
        options.visit = [options.visit]
    if "sensor" in need or "channel" in need:
        if options.raft is None:
            print >>sys.stderr, "Running over all rafts"
            options.raft = [x[0] for x in
                    inButler.queryMetadata("raw", "raft", ("raft",))]
        elif not hasattr(options.raft, "__iter__"):
            options.raft = [options.raft]
    if "sensor" in need or "channel" in need:
        if options.sensor is None:
            print >>sys.stderr, "Running over all sensors"
            options.sensor = [x[0] for x in
                    inButler.queryMetadata("raw", "sensor", ("sensor",))]
        elif not hasattr(options.sensor, "__iter__"):
            options.sensor = [options.sensor]
    if "channel" in need:
        if options.channel is None:
            print >>sys.stderr, "Running over all channels"
            options.channel = [x[0] for x in
                    inButler.queryMetadata("raw", "channel", ("channel",))]
        elif not hasattr(options.channel, "__iter__"):
            options.channel = [options.channel]

    for visit in options.visit:
        if "sensor" in need or "channel" in need:
            for raft in options.raft:
                for sensor in options.sensor:
                    if "channel" in need:
                        for channel in options.channel:
                            if not outButler.fileExists(outDatasetType,
                                    visit=visit, raft=raft, sensor=sensor,
                                    channel=channel):
                                print >>sys.stderr, \
                                        "***** Processing visit %d raft %s sensor %s channel %s" % \
                                        (visit, raft, sensor, channel)
                                processFunction(inButler=inButler,
                                        outButler=outButler,
                                        visit=visit, raft=raft,
                                        sensor=sensor, channel=channel)
                    else:
                        if not outButler.fileExists(outDatasetType,
                                visit=visit, raft=raft, sensor=sensor):
                            print >>sys.stderr, \
                                    "***** Processing visit %d raft %s sensor %s" % \
                                    (visit, raft, sensor)
                            processFunction(inButler=inButler,
                                    outButler=outButler, visit=visit,
                                    raft=raft, sensor=sensor)
        else:
             if not outButler.fileExists(outDatasetType, visit=visit):
                 print >>sys.stderr, "***** Processing visit %d" % (visit,)
                 processFunction(inButler=inButler, outButler=outButler,
                         visit=visit)

def cfhtSetup(root, outRoot, registry, calibRoot, inButler, outButler):
    if inButler is None:
        if calibRoot is None:
            calibRoot = "/lsst/DC3/data/obstest/CFHTLS/calib"
        if registry is None and root is not None:
            if os.path.exists(os.path.join(root, "registry.sqlite3")):
                registry = os.path.join(root, "registry.sqlite3")
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(
            root=root, calibRoot=calibRoot, registry=registry))
        inButler = bf.create()
    if outButler is None:
        if outRoot is None:
            outRoot = root
        obf = dafPersist.ButlerFactory(mapper=CfhtMapper(
            root=outRoot, registry=registry))
        outButler = obf.create() 
    return (inButler, outButler)

def lsstSimSetup(root, outRoot, registry, calibRoot, inButler, outButler):
    if inButler is None:
        if calibRoot is None:
            calibRoot = "/lsst/DC3/data/obstest/ImSim"
        if registry is None and root is not None:
            if os.path.exists(os.path.join(root, "registry.sqlite3")):
                registry = os.path.join(root, "registry.sqlite3")
        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
            root=root, calibRoot=calibRoot, registry=registry))
        inButler = bf.create()
    if outButler is None:
        if outRoot is None:
            outRoot = root
        obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(
            root=outRoot, registry=registry))
        outButler = obf.create() 
    return (inButler, outButler)
