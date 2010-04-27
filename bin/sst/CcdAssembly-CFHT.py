#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def ccdAssemblyProcess(root, outRoot, **keys):
    registry="/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"
    bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root,
        registry=registry))
    butler = bf.create()
    expList = []
    for amp in (0, 1):
        expList.append(butler.get("postISR", amp=amp, **keys))

    clip = {
        'exposureList': expList
    }

    asmb = SimpleStageTester(ipPipe.IsrCcdAssemblyStage(pexPolicy.Policy()))

    clip = asmb.runWorker(clip)
    exposure = clip['assembledCcdExposure']
    # exposure.writeFits("postISRCCD.fits")

    obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot,
            registry=registry))
    outButler = obf.create()
    outButler.put(exposure, "postISRCCD", **keys)

def run():
    # root = "/lsst/DC3/data/obstest/CFHTLS"
    root = "."
    ccdAssemblyProcess(root=root, outRoot=".", visit=788965, ccd=6)

if __name__ == "__main__":
    run()
