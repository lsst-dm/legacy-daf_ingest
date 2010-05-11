#!/usr/bin/env python

# Requires obs_lsstSim 3.0.3

import os
import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def crSplitProcess(root=None, outRoot=None, inButler=None, outButler=None,
        **keys):

    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
        outButler = obf.create()

    clip = {
        'isrCcdExposure0': inButler.get("postISRCCD", snap=0, **keys),
#        'isrCcdExposure1': inButler.get("postISRCCD", snap=1, **keys)
    }

    # bbox = afwImage.BBox(afwImage.PointI(0,0), 2500, 2500)
    # clip['isrCcdExposure0'] = \
    #         afwImage.ExposureF(clip['isrCcdExposure0'], bbox)
    # clip['isrCcdExposure1'] = \
    #         afwImage.ExposureF(clip['isrCcdExposure1'], bbox)

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrCcdExposure0
        }
        outputKeys: {
            backgroundSubtractedExposure: bkgSubCcdExposure0
        }
        parameters: {
            subtractBackground: true
            backgroundPolicy: {
                binsize: 512
            }
        }
        """))
    bkgd0 = SimpleStageTester(measPipe.BackgroundEstimationStage(pol))

#     pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
#         """#<?cfg paf policy?>
#         inputKeys: {
#             exposure: isrCcdExposure1
#         }
#         outputKeys: {
#             backgroundSubtractedExposure: bkgSubCcdExposure1
#         }
#         parameters: {
#             subtractBackground: true
#             backgroundPolicy: {
#                 binsize: 512
#             }
#         }
#         """))
#     bkgd1 = SimpleStageTester(measPipe.BackgroundEstimationStage(pol))
# 
    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: bkgSubCcdExposure0
        }
        outputKeys: {
            exposure: crSubCcdExposure0
        }
        parameters: {
            defaultFwhm: 1.0
            keepCRs: false
        }
        crRejectPolicy: {
            nCrPixelMax: 100000
            # Temporary increase in sigma until gain/variance issues are solved
            minSigma: 10.0
        }
        """))
    cr0 = SimpleStageTester(ipPipe.CrRejectStage(pol))
# 
#     pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
#         """#<?cfg paf policy?>
#         inputKeys: {
#             exposure: bkgSubCcdExposure1
#         }
#         outputKeys: {
#             exposure: crSubCcdExposure1
#         }
#         parameters: {
#             defaultFwhm: 1.0
#             keepCRs: false
#         }
#         crRejectPolicy: {
#             nCrPixelMax: 100000
#         }
#         """))
#     cr1 = SimpleStageTester(ipPipe.CrRejectStage(pol))
# 
#     pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
#         """#<?cfg paf policy?>
#         inputKeys: {
#             exposures: "crSubCcdExposure0" "crSubCcdExposure1"
#         }
#         outputKeys: {
#             differenceExposure: diffExposure
#         }
#         """))
#     diff = SimpleStageTester(ipPipe.SimpleDiffImStage(pol))
# 
#     pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
#         """#<?cfg paf policy?>
#         inputKeys: {
#             exposure: diffExposure
#         }
#         outputKeys: {
#             positiveDetection: positiveFootprintSet
#             negativeDetection: negativeFootprintSet
#             psf: psf
#         }
#         psfPolicy: {
#             parameter: 1.5
#         }
#         backgroundPolicy: {
#             algorithm: NONE
#         }
#         detectionPolicy: {
#             minPixels: 1
#             nGrow: 0
#             thresholdValue: 10.0
#             thresholdType: stdev
#             thresholdPolarity: both
#         }
#         """))
#     srcd = SimpleStageTester(measPipe.SourceDetectionStage(pol))
# 
#     pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
#         """#<?cfg paf policy?>
#         inputKeys: {
#             exposures: "crSubCcdExposure0" "crSubCcdExposure1"
#             positiveDetection: positiveFootprintSet
#             negativeDetection: negativeFootprintSet
#         }
#         outputKeys: {
#             combinedExposure: visitExposure
#         }
#         """))
#     comb = SimpleStageTester(ipPipe.CrSplitCombineStage(pol))

    clip = bkgd0.runWorker(clip)
#     clip = bkgd1.runWorker(clip)
#     clip['bkgSubCcdExposure0'].writeFits("bkgSub0.fits")
#     clip['bkgSubCcdExposure1'].writeFits("bkgSub1.fits")
    clip = cr0.runWorker(clip)
    print clip['nCR']
#     clip = cr1.runWorker(clip)
#     print clip['nCR']
#     clip['crSubCcdExposure0'].writeFits("crSub0.fits")
#     clip['crSubCcdExposure1'].writeFits("crSub1.fits")
#     clip = diff.runWorker(clip)
#     clip['diffExposure'].writeFits("diff.fits")
#     clip = srcd.runWorker(clip)
#     clip = comb.runWorker(clip)
#     clip['visitExposure'].writeFits("visit.fits")
    # clip = sst.runWorker(clip)


    # exposure = clip['visitExposure']
    # exposure = clip['bkgSubCcdExposure0']
    exposure = clip['crSubCcdExposure0']
    # exposure.writeFits("visitim.fits")

    outButler.put(exposure, "visitim", **keys)

def run():
    root = os.path.join(os.environ['AFWDATA_DIR'], "ImSim")
    crSplitProcess(root=root, outRoot=".",
            visit=85751839, raft="2,3", sensor="1,1")

if __name__ == "__main__":
    run()
