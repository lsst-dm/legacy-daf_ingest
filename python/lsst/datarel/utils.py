from optparse import OptionParser
import os
import sys

import lsst.pex.policy as pexPolicy
import lsst.daf.persistence as dafPersist
from lsst.pex.harness.simpleStageTester import SimpleStageTester

haveCfht = True
haveLsstSim = True
try:
    from lsst.obs.cfht import CfhtMapper
except:
    haveCfht = False
try:
    from lsst.obs.lsstSim import LsstSimMapper
except:
    haveLsstSim = False


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
    parser.add_option("-f", "--force", action="store_true", default=False,
            help="execute even if output dataset exists")
    if "calib" in need:
        parser.add_option("-C", "--calibRoot", dest="calibRoot",
                help="calibration root")
    parser.add_option("-R", "--registry", help="registry")
    if "skyTile" in need:
        parser.add_option("-t", "--skyTile", action="append", type="int",
                help="sky tile numbers (can be repeated)")
    else:
        parser.add_option("-v", "--visit", action="append", type="int",
                help="visit numbers (can be repeated)")
        if "ccd" in need or "amp" in need:
            parser.add_option("-c", "--ccd", action="append", type="int",
                    help="ccd number (can be repeated)")
        if "amp" in need:
            parser.add_option("-a", "--amp", action="append", type="int",
                    help="amp number (can be repeated)")
    (options, args) = parser.parse_args()

    if options.registry is None:
        if os.path.exists(os.path.join(options.root, "registry.sqlite3")):
            options.registry = os.path.join(options.root, "registry.sqlite3")
    if options.registry is None:
        if os.path.exists("/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"):
            options.registry = "/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"
    if "calib" in need:
        if options.calibRoot is None:
            if os.path.exists("/lsst/DC3/data/obstest/CFHTLS/calib"):
                options.calibRoot = "/lsst/DC3/data/obstest/CFHTLS/calib"
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

    if "skyTile" in need:
        if options.skyTile is None:
            print >>sys.stderr, "Running over all sky tiles"
            options.skyTile = inButler.queryMetadata("raw", "skyTile")
        elif not hasattr(options.skyTile, "__iter__"):
            options.skyTile = [options.skyTile]
        for skyTile in options.skyTile:
            if options.force or not outButler.datasetExists(outDatasetType,
                    skyTile=skyTile):
                print >>sys.stderr, \
                        "***** Processing skyTile %d" % (skyTile,)
                processFunction(inButler=inButler, outButler=outButler,
                        skyTile=skyTile)
        return

    if options.visit is None:
        print >>sys.stderr, "Running over all input visits"
        options.visit = inButler.queryMetadata("raw", "visit")
    elif not hasattr(options.visit, "__iter__"):
        options.visit = [options.visit]
    if "ccd" in need or "amp" in need:
        if options.ccd is None:
            print >>sys.stderr, "Running over all CCDs"
            options.ccd = inButler.queryMetadata("raw", "ccd")
        elif not hasattr(options.ccd, "__iter__"):
            options.ccd = [options.ccd]
    if "amp" in need:
        if options.amp is None:
            print >>sys.stderr, "Running over all amps"
            options.amp = inButler.queryMetadata("raw", "amp")
        elif not hasattr(options.amp, "__iter__"):
            options.amp = [options.amp]

    for visit in options.visit:
        if "ccd" in need or "amp" in need:
            for ccd in options.ccd:
                if "amp" in need:
                    for amp in options.amp:
                        if options.force or \
                                not outButler.datasetExists(outDatasetType,
                                        visit=visit, ccd=ccd, amp=amp):
                            print >>sys.stderr, \
                                    "***** Processing visit %d ccd %d amp %d" % \
                                    (visit, ccd, amp)
                            processFunction(inButler=inButler,
                                    outButler=outButler,
                                    visit=visit, ccd=ccd, amp=amp)
                else:
                    if options.force or \
                            not outButler.datasetExists(outDatasetType,
                                    visit=visit, ccd=ccd):
                        print >>sys.stderr, \
                                "***** Processing visit %d ccd %d" % \
                                (visit, ccd)
                        processFunction(inButler=inButler, outButler=outButler,
                                visit=visit, ccd=ccd)
        else:
            if options.force or \
                    not outButler.datasetExists(outDatasetType, visit=visit):
                print >>sys.stderr, "***** Processing visit %d" % (visit,)
                processFunction(inButler=inButler, outButler=outButler,
                        visit=visit)
            

def lsstSimMain(processFunction, outDatasetType, need=(), defaultRoot="."):
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="root",
            default=defaultRoot, help="input root")
    parser.add_option("-o", "--output", dest="outRoot", default=".",
            help="output root")
    parser.add_option("-f", "--force", action="store_true", default=False,
            help="execute even if output dataset exists")
    if "calib" in need:
        parser.add_option("-C", "--calibRoot", dest="calibRoot",
                help="calibration root")
    parser.add_option("-R", "--registry", help="registry")
    if "skyTile" in need:
        parser.add_option("-t", "--skyTile", action="append", type="int",
                help="sky tile numbers (can be repeated)")
    else:
        parser.add_option("-v", "--visit", action="append", type="int",
                help="visit number (can be repeated)")
        if "snap" in need:
            parser.add_option("-S", "--snap", action="append", type="int",
                    help="snap number (can be repeated)")
        if "sensor" in need or "channel" in need:
            parser.add_option("-r", "--raft", action="append",
                    help="raft coords (can be repeated)")
            parser.add_option("-s", "--sensor", action="append",
                    help="sensor coords (can be repeated)")
        if "channel" in need:
            parser.add_option("-a", "--channel", action="append",
                    help="channel coords (can be repeated)")
    (options, args) = parser.parse_args()

    if options.registry is None:
        if os.path.exists(os.path.join(options.root, "registry.sqlite3")):
            options.registry = os.path.join(options.root, "registry.sqlite3")
        elif os.path.exists("/lsst/DC3/data/obstest/ImSim/registry.sqlite3"):
            options.registry = "/lsst/DC3/data/obstest/ImSim/registry.sqlite3"
    if "calib" in need:
        if os.path.exists("/lsst/DC3/data/obstest/ImSim"):
            options.calibRoot = "/lsst/DC3/data/obstest/ImSim"
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
        options.visit = inButler.queryMetadata("raw", "visit")
    elif not hasattr(options.visit, "__iter__"):
        options.visit = [options.visit]

    if "skyTile" in need:
        if options.skyTile is None:
            print >>sys.stderr, "Running over all sky tiles"
            options.skyTile = inButler.queryMetadata("raw", "skyTile")
        elif not hasattr(options.skyTile, "__iter__"):
            options.skyTile = [options.skyTile]
        for skyTile in options.skyTile:
            if options.force or not outButler.datasetExists(outDatasetType,
                    skyTile=skyTile):
                print >>sys.stderr, \
                        "***** Processing skyTile %d" % (skyTile,)
                processFunction(inButler=inButler, outButler=outButler,
                        skyTile=skyTile)
        return

    if "snap" in need:
        if options.snap is None:
            print >>sys.stderr, "Running over all snaps"
            options.snap = inButler.queryMetadata("raw", "snap")
        elif not hasattr(options.snap, "__iter__"):
            options.snap = [options.snap]
    else:
        setattr(options, "snap", [0])

    if "sensor" in need or "channel" in need:
        if options.raft is None:
            print >>sys.stderr, "Running over all rafts"
            options.raft = inButler.queryMetadata("raw", "raft")
        elif not hasattr(options.raft, "__iter__"):
            options.raft = [options.raft]

    if "sensor" in need or "channel" in need:
        if options.sensor is None:
            print >>sys.stderr, "Running over all sensors"
            options.sensor = inButler.queryMetadata("raw", "sensor")
        elif not hasattr(options.sensor, "__iter__"):
            options.sensor = [options.sensor]

    if "channel" in need:
        if options.channel is None:
            print >>sys.stderr, "Running over all channels"
            options.channel = inButler.queryMetadata("raw", "channel")
        elif not hasattr(options.channel, "__iter__"):
            options.channel = [options.channel]

    for visit in options.visit:
        if "sensor" in need or "channel" in need:
            if "snap" in need:
                for snap in options.snap:
                    for raft in options.raft:
                        for sensor in options.sensor:
                            if "channel" in need:
                                for channel in options.channel:
                                    if options.force or \
                                            not outButler.datasetExists(
                                                    outDatasetType,
                                                    visit=visit, snap=snap,
                                                    raft=raft, sensor=sensor,
                                                    channel=channel):
                                        print >>sys.stderr, \
                                                ("***** Processing " + \
                                                "visit %d snap %d raft %s " + \
                                                "sensor %s channel %s") % \
                                                (visit, snap, raft, sensor,
                                                        channel)
                                        processFunction(inButler=inButler,
                                                outButler=outButler,
                                                visit=visit, snap=snap,
                                                raft=raft, sensor=sensor,
                                                channel=channel)
                            else:
                                if options.force or \
                                        not outButler.datasetExists(
                                                outDatasetType,
                                                visit=visit, snap=snap,
                                                raft=raft, sensor=sensor):
                                    print >>sys.stderr, \
                                            ("***** Processing visit %d " + \
                                            "snap %d raft %s sensor %s") % \
                                            (visit, snap, raft, sensor)
                                    processFunction(inButler=inButler,
                                            outButler=outButler, visit=visit,
                                            snap=snap, raft=raft, sensor=sensor)
            else: # snap
                for raft in options.raft:
                    for sensor in options.sensor:
                        if "channel" in need:
                            for channel in options.channel:
                                if options.force or \
                                        not outButler.datasetExists(
                                                outDatasetType, visit=visit,
                                                raft=raft, sensor=sensor,
                                                channel=channel):
                                    print >>sys.stderr, \
                                            ("***** Processing visit %d " + \
                                            "raft %s sensor %s channel %s") % \
                                            (visit, raft, sensor, channel)
                                    processFunction(inButler=inButler,
                                            outButler=outButler,
                                            visit=visit, raft=raft,
                                            sensor=sensor, channel=channel)
                        else:
                            if options.force or \
                                    not outButler.datasetExists(outDatasetType,
                                            visit=visit, raft=raft,
                                            sensor=sensor):
                                print >>sys.stderr, \
                                        ("***** Processing visit %d " + \
                                        "raft %s sensor %s") % \
                                        (visit, raft, sensor)
                                processFunction(inButler=inButler,
                                        outButler=outButler, visit=visit,
                                        raft=raft, sensor=sensor)
        else: # raft, sensor
             if options.force or \
                     not outButler.datasetExists(outDatasetType, visit=visit):
                 print >>sys.stderr, "***** Processing visit %d" % (visit,)
                 processFunction(inButler=inButler, outButler=outButler,
                         visit=visit)

def cfhtSetup(root, outRoot, registry, calibRoot, inButler, outButler):
    if inButler is None:
        if calibRoot is None:
            if os.path.exists("/lsst/DC3/data/obstest/CFHTLS/calib"):
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
            if os.path.exists("/lsst/DC3/data/obstest/ImSim"):
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
