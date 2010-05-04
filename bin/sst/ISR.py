#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def isrProcess(root, outRoot, **keys):
    bf = dafPersist.ButlerFactory(
            mapper=LsstSimMapper(
                root=root,
                calibRoot=root)
            )
    butler = bf.create()
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
            saturationMaskedExposure: isrExposure
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
    clip = bias.runWorker(clip)
    clip = dark.runWorker(clip)
    clip = flat.runWorker(clip)
    exposure = clip['isrExposure']
    bboxes = clip['satPixels']
    # exposure.writeFits("postIsr.fits")
    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
    outButler = obf.create()
    outButler.put(exposure, "postISR", **keys)
    outButler.put(bboxes, "satPixelSet", **keys)

def run():
    root = "/lsst/DC3/data/obstest/ImSim"
    if not os.path.exists("registry.sqlite3"):
        os.symlink(os.path.join(root, "registry.sqlite3"),
                "./registry.sqlite3")
    isrProcess(root=root, outRoot=".", visit=85470982, snap=0,
            raft="2,3", sensor="1,1", channel="0,0")

if __name__ == "__main__":
    run()
