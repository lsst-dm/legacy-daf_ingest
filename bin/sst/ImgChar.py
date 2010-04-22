#!/usr/bin/env python

import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def imgCharProcess(root, **keys):
    bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    butler = bf.create()

    clip = {
        'visitExposure': butler.get("visitim", **keys),
    }

    bbox = afwImage.BBox(afwImage.PointI(0,0), 1024, 1024)
    clip['visitExposure'] = \
            afwImage.ExposureF(clip['visitExposure'], bbox)

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: visitExposure
        }
        outputKeys: {
            positiveDetection: positiveFootprintSet
            negativeDetection: negativeFootprintSet
            psf: simplePsf
        }
        psfPolicy: {
            height: 5
            width: 5
            parameter: 1.0
        }
        """))
    srcd = SimpleStageTester(measPipe.SourceDetectionStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: visitExposure
            psf: simplePsf
            positiveDetection: positiveFootprintSet
            negativeDetection: negativeFootprintSet
        }
        outputKeys: {
            sources: sourceSet
        }
        """))
    srcm = SimpleStageTester(measPipe.SourceMeasurementStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: visitExposure
            sourceSet: sourceSet
        }
        outputKeys: {
            psf: measuredPsf
            cellSet: cellSet
            sdqa: sdqa
        }
        """))
    psfd = SimpleStageTester(measPipe.PsfDeterminationStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputExposureKey: visitExposure
        inputSourceSetKey: sourceSet
        outputWcsKey: measuredWcs
        outputMatchListKey: matchList
        """))
    wcsd = SimpleStageTester(measPipe.WcsDeterminationStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        sourceMatchSetKey: matchList
        outputDictKey: wcsVerifyStats
        """))
    wcsv = SimpleStageTester(measPipe.WcsVerificationStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        sourceMatchSetKey: matchList
        outputValueKey: photometricZeroPoint
        outputUncertaintyKey: photometricZeroPointUnc
        """))
    pcal = SimpleStageTester(measPipe.PhotoCalStage(pol))

    clip = srcd.runWorker(clip)
    clip = srcm.runWorker(clip)
    clip = psfd.runWorker(clip)
    clip = wcsd.runWorker(clip)
    clip = wcsv.runWorker(clip)
    clip = pcal.runWorker(clip)

    print clip['measuredPsf'].getKernel().toString()
    print clip['measuredWcs'].getFitsMetadata().toString()
    print clip['wcsVerifyStats']
    print clip['photometricZeroPoint']
    print clip['photometricZeroPointUnc']
    print clip['sdqa']

def run():
    # Needs visitim/v{visit}-f{filter}/R{raft}-S{sensor}.fits, which is not
    # yet in afwdata.  Use a symlink to postISRCCD/v-f/s0/R-S for now.
    imgCharProcess(".", visit=85751839, raft="2,3", sensor="1,1")

if __name__ == "__main__":
    run()
