#!/usr/bin/env python

# Requires obs_lsstSim 3.0.3

import os
import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def crSplitProcess(root, outRoot, **keys):
    registry="/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"
    bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root,
        registry=registry))
    butler = bf.create()

    clip = {
        'isrCcdExposure': butler.get("postISRCCD", **keys),
    }

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrCcdExposure
        }
        outputKeys: {
            backgroundSubtractedExposure: bkgSubCcdExposure
        }
        parameters: {
            subtractBackground: true
            backgroundPolicy: {
                binsize: 512
            }
        }
        """))
    bkgd = SimpleStageTester(measPipe.BackgroundEstimationStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: bkgSubCcdExposure
        }
        outputKeys: {
            exposure: crSubCcdExposure
        }
        parameters: {
            defaultFwhm: 1.0
            keepCRs: false
        }
        crRejectPolicy: {
            nCrPixelMax: 100000
        }
        """))
    cr = SimpleStageTester(ipPipe.CrRejectStage(pol))


    clip = bkgd.runWorker(clip)
    clip['bkgSubCcdExposure'].writeFits("bkgSub.fits")
    clip = cr.runWorker(clip)
    print clip['nCR']
    clip['crSubCcdExposure'].writeFits("crSub.fits")

    exposure = clip['crSubCcdExposure']
    obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot,
        registry=registry))
    outButler = obf.create()
    outButler.put(exposure, "visitim", **keys)

def run():
    root = "."
    crSplitProcess(root=root, outRoot=".", visit=788965, ccd=6)

if __name__ == "__main__":
    run()
