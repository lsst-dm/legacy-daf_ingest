#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.ip.isr as ipIsr
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def ccdAssemblyProcess(root, outRoot, **keys):
    #Fake a set of saturation bounding boxes just to see if everything works
    #as it should
    bboxes = []
    for i in range(750):
        for j in range(5):
            bboxes.append(ipIsr.Bbox(i*2, j*90, 10, 10))
    bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    butler = bf.create()
    expList = []
    for ampX in (0, 1):
        for ampY in xrange(8):
            ampName = "%d,%d" % (ampX, ampY)
            expList.append(butler.get("postISR", channel=ampName, **keys))

    clip = {
        'exposureList': expList,
        'satPixels': bboxes
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

    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
    outButler = obf.create()
    outButler.put(exposure, "postISRCCD", **keys)

def run():
    root = os.path.join(os.environ['AFWDATA_DIR'], "ImSim")
    ccdAssemblyProcess(root=root, outRoot=".",
            visit=85751839, snap=0, raft="2,3", sensor="1,1", filter="r")

if __name__ == "__main__":
    run()
