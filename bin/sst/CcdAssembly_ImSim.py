#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def ccdAssemblyProcess(root=None, outRoot=None, inButler=None, outButler=None,
        **keys):
    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
        outButler = obf.create()

    expList = []
    bboxes = []
    for ampX in (0, 1):
        for ampY in xrange(8):
            ampName = "%d,%d" % (ampX, ampY)
            expList.append(inButler.get("postISR", channel=ampName, **keys))
            # bboxes.append(inButler.get("satPixelSet", channel=ampName, **keys))

    clip = {
        'exposureList': expList,
        # 'satPixels': bboxes
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
    root = os.path.join(os.environ['AFWDATA_DIR'], "ImSim")
    ccdAssemblyProcess(root=root, outRoot=".",
            visit=85751839, snap=0, raft="2,3", sensor="1,1", filter="r")

if __name__ == "__main__":
    run()
