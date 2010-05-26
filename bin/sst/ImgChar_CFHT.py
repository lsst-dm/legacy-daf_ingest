#!/usr/bin/env python

import os
import sys

from lsst.datarel import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
from stageCtrl import *

def imgCharProcess(root=None, outRoot=None, registry=None,
                   inButler=None, outButler=None, stages=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, None,
                                    inButler, outButler)
    #
    # Which stages to run, and prerequisites
    #
    stages = setPrerequisites(stages)

    if not os.environ.has_key("ASTROMETRY_NET_DATA_DIR") or \
            os.environ['ASTROMETRY_NET_DATA_DIR'].find("cfhtlsDeep") == -1:
        msg = "astrometry_net_data is not setup to cfhtlsDeep"
        if stages & WCS:
            raise RuntimeError, msg
        else:
            print >> sys.stderr, msg

    clip = {
        'visitExposure': inButler.get("visitim", **keys),
    }

    if stages & DETECT:
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

    if stages & MEASURE:
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
        outButler.put(clip['sourceSet_persistable'], "icSrc", **keys)

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

    if stages & PSF:
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

    if stages & WCS:
        clip = runStage(measPipe.WcsDeterminationStage,
            """#<?cfg paf policy?>
            inputExposureKey: visitExposure
            inputSourceSetKey: sourceSet
            outputWcsKey: measuredWcs
            outputMatchListKey: matchList
            numBrightStars: 75
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

    if stages & WCS_VERIFY:
        clip = runStage(measPipe.WcsVerificationStage,
            """#<?cfg paf policy?>
            sourceMatchSetKey: matchList
            outputDictKey: wcsVerifyStats
            """, clip)

        print >>sys.stderr, "WCS verify:", clip['wcsVerifyStats']

    if stages & PHOTO_CAL:
        clip = runStage(measPipe.PhotoCalStage,
            """#<?cfg paf policy?>
            sourceMatchSetKey: matchList
            outputValueKey: photometricMagnitudeObject
            """, clip)

        photoObj = clip['photometricMagnitudeObject']
        print >>sys.stderr, "Photometric zero:", photoObj.getMag(1)
        print >>sys.stderr, "Flux of a 20th mag object:", photoObj.getFlux(20)

        outButler.put(clip['visitExposure'], "calexp", **keys)

def test(root=".", outRoot=".", visit=788965, ccd=6, stages=None):
    """Run the specified visit/ccd.  If stages is omitted (or None) all available stages will be run"""
    imgCharProcess(root=root, outRoot=outRoot, visit=visit, ccd=ccd, stages=stages)

def main():
    cfhtMain(imgCharProcess, "calexp", "ccd")

if __name__ == "__main__":
    main()
