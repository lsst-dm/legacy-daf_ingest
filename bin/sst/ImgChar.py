#!/usr/bin/env python

import os
import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester
from lsst.pex.harness.IOStage import OutputStage

def imgCharProcess(root, outRoot, **keys):
    bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    butler = bf.create()

    clip = {
        'visitExposure': butler.get("visitim", **keys),
    }

    # bbox = afwImage.BBox(afwImage.PointI(0,0), 1024, 1024)
    # clip['visitExposure'] = \
    #         afwImage.ExposureF(clip['visitExposure'], bbox)

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
        numBrightStars: 75
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

    fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr",
            "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
            "IyyErr", "Ixy", "IxyErr")
    csv = open(os.path.join(outRoot, "imgCharSources.csv"), "w")
    print >>csv, "FlagForDetection," + ",".join(fields)
    for s in clip['sourceSet']:
        line = "%d" % (s.getFlagForDetection(),)
        for f in fields:
            func = getattr(s, "get" + f)
            line += ",%f" % (func(),)
        print >>csv, line
    csv.close()

    clip = psfd.runWorker(clip)
    print clip['measuredPsf'].getKernel().toString()

    obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
    outButler = obf.create()
    outButler.put(clip['measuredPsf'], "psf", **keys)

    clip = wcsd.runWorker(clip)
    print clip['measuredWcs'].getFitsMetadata().toString()

    clip = wcsv.runWorker(clip)
    print clip['wcsVerifyStats']

    clip = pcal.runWorker(clip)
    print "Photometric zero:", clip['photometricZeroPoint']
    print "Photometric zero unc:", clip['photometricZeroPointUnc']

def run():
    imgCharProcess(
            root=".",
    #        root=os.path.join(os.environ['AFWDATA_DIR'], "ImSim"),
            outRoot=".", visit=85751839, raft="2,3", sensor="1,1")

if __name__ == "__main__":
    run()
