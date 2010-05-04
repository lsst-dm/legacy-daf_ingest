#!/usr/bin/env python

import os
import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester
from lsst.pex.harness.IOStage import OutputStage

def imgCharProcess(root, outRoot, **keys):
    registry="/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"
    bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root,
        registry=registry))
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

    obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot,
        registry=registry))
    outButler = obf.create()
    outButler.put(clip['measuredPsf'], "psf", **keys)

    clip = wcsd.runWorker(clip)
    print clip['measuredWcs'].getFitsMetadata().toString()

    clip = wcsv.runWorker(clip)
    print clip['wcsVerifyStats']

    clip = pcal.runWorker(clip)
    print clip['photometricZeroPoint']
    print clip['photometricZeroPointUnc']

    outButler.put(clip['visitExposure'], "calexp", **keys)

def run():
    imgCharProcess(root=".", outRoot=".", visit=788965, ccd=6)

if __name__ == "__main__":
    run()
