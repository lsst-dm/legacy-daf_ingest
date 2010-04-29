#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def isrProcess(butler, outButler, **keys):
    clip = {
        'isrExposure': butler.get("raw", **keys),
        'biasExposure': butler.get("bias", **keys),
        'darkExposure': butler.get("dark", **keys),
        'flatExposure': butler.get("flat", **keys)
    }

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            saturationCorrectedExposure: isrExposure
        }
        """))
    sat = SimpleStageTester(ipPipe.IsrSaturationStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            overscanCorrectedExposure: isrExposure
        }
        """))
    over = SimpleStageTester(ipPipe.IsrOverscanStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            biasexposure: biasExposure
        }
        outputKeys: {
            biasSubtractedExposure: isrExposure
        }
        """))
    bias = SimpleStageTester(ipPipe.IsrBiasStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            darkexposure: darkExposure
        }
        outputKeys: {
            darkSubtractedExposure: isrExposure
        }
        """))
    dark = SimpleStageTester(ipPipe.IsrDarkStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            flatexposure: flatExposure
        }
        outputKeys: {
            flatCorrectedExposure: isrExposure
        }
        """))
    flat = SimpleStageTester(ipPipe.IsrFlatStage(pol))

    clip = sat.runWorker(clip)
    clip = over.runWorker(clip)
    #clip = bias.runWorker(clip)
    #clip = dark.runWorker(clip)
    #clip = flat.runWorker(clip)
    exposure = clip['isrExposure']
    outButler.put(exposure, "postISR", **keys)

def run():
    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "rawbias/imsim_%(visit)d_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "bias/v0/R%(raft)s/S%(sensor)s/imsim_0_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
        """))
    root = os.path.join(os.environ['AFWDATA_DIR'], "imsim_tmp")
    bf = dafPersist.ButlerFactory(
            mapper=LsstSimMapper(
                policy=pol,
                root=root,
                calibRoot=root
            ))
    butler = bf.create()
    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    outButler = obf.create()
    for i in range(0,2):
        for j in range(0,8):
            isrProcess(butler, butler, visit=0, snap=0,
                raft="2,3", sensor="1,1", channel="%i,%i"%(i,j), filter="r")

if __name__ == "__main__":
    run()
