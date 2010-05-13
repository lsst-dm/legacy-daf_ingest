#!/usr/bin/env python

# Requires obs_lsstSim 3.0.3

import os
import sys
import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def crSplitProcess(root=None, outRoot=None, inButler=None, outButler=None,
        **keys):

    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot))
        outButler = obf.create()

    clip = {
        'isrCcdExposure': inButler.get("postISRCCD", **keys),
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
    print >>sys.stderr, clip['nCR'], "cosmic rays"

    exposure = clip['crSubCcdExposure']
    outButler.put(exposure, "visitim", **keys)

def run():
    root = "."
    crSplitProcess(root=root, outRoot=".", visit=788965, ccd=6)

if __name__ == "__main__":
    run()
