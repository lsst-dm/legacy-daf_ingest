#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def ccdAssemblyProcess(root, outRoot, **keys):
    bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    butler = bf.create()
    expList = []
    for ampX in (0, 1):
        for ampY in xrange(8):
            ampName = "C:%d,%d" % (ampX, ampY)
            expList.append(butler.get("postISR", channel=ampName, **keys))

    clip = {
        'exposureList': expList
    }

    asmb = SimpleStageTester(ipPipe.IsrCcdAssemblyStage(pexPolicy.Policy()))

    clip = asmb.runWorker(clip)
    exposure = clip['assembledCcdExposure']
    # exposure.writeFits("postISRCCD.fits")

    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
    outButler = obf.create()
    outButler.put(exposure, "postISRCCD", **keys)

def run():
    root = os.path.join(os.environ['AFWDATA_DIR'], "ImSim")
    ccdAssemblyProcess(root=root, outRoot=".",
            visit=85751839, snap=0, raft="R:2,3", sensor="S:1,1")

if __name__ == "__main__":
    run()
