#!/usr/bin/env python

import os
import sys

from utils import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe

def imgCharProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, None,
            inButler, outButler)

    if not os.environ.has_key("ASTROMETRY_NET_DATA_DIR") or \
            os.environ['ASTROMETRY_NET_DATA_DIR'].find("cfhtlsDeep") == -1:
        raise RuntimeError, "astrometry_net_data is not setup to cfhtlsDeep"

    clip = {
        'visitExposure': inButler.get("visitim", **keys),
    }

    clip = runStage(measPipe.SourceDetectionStage,
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
        backgroundPolicy: {
            algorithm: NONE
        }
        """, clip)

    clip = runStage(measPipe.SourceMeasurementStage,
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
        """, clip)

    fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr",
            "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
            "IyyErr", "Ixy", "IxyErr")
    csv = open("imgCharSources-v%(visit)d-c%(ccd)d.csv" % keys, "w")
    print >>csv, "FlagForDetection," + ",".join(fields)
    for s in clip['sourceSet']:
        line = "%d" % (s.getFlagForDetection(),)
        for f in fields:
            func = getattr(s, "get" + f)
            line += ",%g" % (func(),)
        print >>csv, line
    csv.close()

    clip = runStage(measPipe.PsfDeterminationStage,
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
        """, clip)

    print >>sys.stderr, "PSF:", clip['measuredPsf'].getKernel().toString()
    outButler.put(clip['measuredPsf'], "psf", **keys)

    clip = runStage(measPipe.WcsDeterminationStage,
        """#<?cfg paf policy?>
        inputExposureKey: visitExposure
        inputSourceSetKey: sourceSet
        outputWcsKey: measuredWcs
        outputMatchListKey: matchList
        numBrightStars: 75
        wcsToleranceInArcsec: 0.3
        defaultFilterName: mag
        """, clip)

    print >>sys.stderr, "WCS:", clip['measuredWcs'].getFitsMetadata().toString()
    csv = open("wcsMatches-v%(visit)d-c%(ccd)d.csv" % keys, "w")
    print >>csv, "CatRA,CatDec,CatPsfFlux," + \
            "ImgRA,ImgDec,ImgPsfFlux,Distance"
    for m in clip['matchList']:
        print >>csv, "%f,%f,%g,%f,%f,%g,%f" % (
                m.first.getRa(), m.first.getDec(),
                m.first.getPsfFlux(),
                m.second.getRa(), m.second.getDec(),
                m.second.getPsfFlux(),
                m.distance)
    csv.close()

    clip = runStage(measPipe.WcsVerificationStage,
        """#<?cfg paf policy?>
        sourceMatchSetKey: matchList
        outputDictKey: wcsVerifyStats
        """, clip)

    print >>sys.stderr, "WCS verify:", clip['wcsVerifyStats']

    clip = runStage(measPipe.PhotoCalStage,
        """#<?cfg paf policy?>
        sourceMatchSetKey: matchList
        outputValueKey: photometricMagnitudeObject
        """, clip)

    photoObj = clip['photometricMagnitudeObject']
    print >>sys.stderr, "Photometric zero:", photoObj.getMag(1)
    print >>sys.stderr, "Flux of a 20th mag object:", photoObj.getFlux(20)

    outButler.put(clip['visitExposure'], "calexp", **keys)

def test():
    imgCharProcess(root=".", outRoot=".", visit=788965, ccd=6)

def main():
    cfhtMain(imgCharProcess, "calexp", "ccd")

if __name__ == "__main__":
    main()
