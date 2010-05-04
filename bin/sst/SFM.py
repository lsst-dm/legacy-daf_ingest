#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def sfmProcess(root, outRoot, **keys):
    bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    butler = bf.create()

    clip = {
        'scienceExposure': butler.get("calexp", **keys),
        'psf': butler.get("psf", **keys)
    }

    pol = pexPolicy.Policy()
    pol.set("inputKeys.exposure", "scienceExposure")
    pol.set("inputKeys.psf", "psf")
    pol.set("outputKeys.positiveDetection", "positiveFootprintSet")
    # pol.set("outputKeys.psf", "psf") # Remove this eventually
    # pol.set("psfPolicy.parameter", 1.5)
    srcd = SimpleStageTester(measPipe.SourceDetectionStage(pol))

    pol = pexPolicy.Policy()
    pol.set("inputKeys.exposure", "scienceExposure")
    pol.set("inputKeys.psf", "psf")
    pol.set("inputKeys.positiveDetection", "positiveFootprintSet")
    pol.set("outputKeys.sources", "sourceSet")
    srcm = SimpleStageTester(measPipe.SourceMeasurementStage(pol))

    clip = srcd.runWorker(clip)
    clip = srcm.runWorker(clip)

    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
    outButler = obf.create()
    outButler.put(clip['sourceSet_persistable'], "src", **keys)

    fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr", 
            "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
            "IyyErr", "Ixy", "IxyErr")
    csv = open(os.path.join(outRoot, "sources.csv"), "w")
    print >>csv, "FlagForDetection," + ",".join(fields)
    for s in clip['sourceSet']:
        line = "%d" % (s.getFlagForDetection(),)
        for f in fields:
            func = getattr(s, "get" + f)
            line += ",%f" % (func(),)
        print >>csv, line
    csv.close()

def run():
    sfmProcess(root=".", outRoot=".", visit=85751839, raft="2,3", sensor="1,1")

if __name__ == "__main__":
    run()
