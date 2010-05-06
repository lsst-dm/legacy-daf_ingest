#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def ccdAssemblyProcess(root=None, outRoot=None, inButler=None, outButler=None,
        **keys):
    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot))
        outButler = obf.create()

    expList = []
    for amp in (0, 1):
        expList.append(inButler.get("postISR", amp=amp, **keys))

    clip = {
        'exposureList': expList
    }

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        outputKeys: {
            assembledCcdExposure: isrExposure
        }
        """))
    asmb = SimpleStageTester(ipPipe.IsrCcdAssemblyStage(pol))
    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            ccdExposure: isrExposure
        }
        """))
    defect = SimpleStageTester(ipPipe.IsrCcdDefectStage(pol))

    clip = asmb.runWorker(clip)
    clip = defect.runWorker(clip)
    exposure = clip['defectMaskedCcdExposure']
    # exposure.writeFits("postISRCCD.fits")

    outButler.put(exposure, "postISRCCD", **keys)

def run():
    # root = "/lsst/DC3/data/obstest/CFHTLS"
    root="."
    ccdAssemblyProcess(root=root, outRoot=".", visit=788965, ccd=0)

if __name__ == "__main__":
    run()
