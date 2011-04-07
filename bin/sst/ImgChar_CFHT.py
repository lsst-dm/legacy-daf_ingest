#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#


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

    visitim = inButler.get("visitim", **keys)

    clip = imgCharPipe(visitim, stages)

    outButler.put(clip['sourceSet_persistable'], "icSrc", **keys)
    outButler.put(clip['measuredPsf'], "psf", **keys)
    outButler.put(clip['visitExposure'], "calexp", **keys)

def imgCharPipe(visitim, stages=None):
    #
    # Which stages to run, and prerequisites
    #
    stages = setPrerequisites(stages)

    if not os.environ.has_key("SETUP_ASTROMETRY_NET_DATA") or \
            os.environ['SETUP_ASTROMETRY_NET_DATA'].find("cfht") == -1:
        msg = "astrometry_net_data is not setup to a cfht catalog"
        if stages & WCS:
            raise RuntimeError, msg
        else:
            print >> sys.stderr, msg

    clip = {
        'visitExposure': visitim
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
                height: 15
                width: 15
                parameter: 2.12
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

#         fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr",
#                 "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
#                 "IyyErr", "Ixy", "IxyErr")
#         csv = open("imgCharSources-v%(visit)d-c%(ccd)d.csv" % keys, "w")
#         print >>csv, "FlagForDetection," + ",".join(fields)
#         for s in clip['sourceSet']:
#             line = "%d" % (s.getFlagForDetection(),)
#             for f in fields:
#                 func = getattr(s, "get" + f)
#                 line += ",%g" % (func(),)
#             print >>csv, line
#         csv.close()

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

#        print >>sys.stderr, "PSF:", clip['measuredPsf'].getKernel().toString()


    if stages & APCORR:
        clip = runStage(measPipe.ApertureCorrectionStage,
            """#<?cfg paf policy?>
            inputKeys: {
                exposure: visitExposure
                cellSet: cellSet
            }
            outputKeys: {
                apCorr: apCorr
                sdqa: sdqaApCorr
            }
            """, clip)

    if stages & APCORR_APPLY:
        clip = runStage(measPipe.ApertureCorrectionApplyStage,
            """#<?cfg paf policy?>
            inputKeys: {
                sourceSet: sourceSet
                apCorr: apCorr
            }
            """, clip)



    if stages & WCS:
        clip = runStage(measPipe.WcsDeterminationStage,
            """#<?cfg paf policy?>
            inputExposureKey: visitExposure
            inputSourceSetKey: sourceSet
            outputWcsKey: measuredWcs
            outputMatchListKey: matchList
            numBrightStars: 75
            defaultFilterName: mag
            distanceForCatalogueMatchinArcsec: 1.5
            """, clip)

        if False:
            print >>sys.stderr, "WCS:", clip['measuredWcs'].getFitsMetadata().toString()
            csv = open("wcsMatches-v%(visit)d-c%(ccd)02d.csv" % keys, "w")
            print >>csv, "id1,x1,y1," + "CatRA,CatDec,CatPsfFlux," + \
                    "ImgRA,ImgDec,ImgPsfFlux,Distance" + ",id2,x2,y2"
            for m in clip['matchList']:
                print >>csv, ("%d,%f,%f," + "%f,%f,%g,%f,%f,%g,%f" + ",%d,%f,%f") % (
                    m.first.getId(), m.first.getXAstrom(), m.first.getYAstrom(),
                    m.first.getRa(), m.first.getDec(),
                    m.first.getPsfFlux(),
                    m.second.getRa(), m.second.getDec(),
                    m.second.getPsfFlux(),
                    m.distance,
                    m.second.getId(), m.second.getXAstrom(), m.second.getYAstrom(),
                    )
            csv.close()

    if stages & WCS_VERIFY:
        clip = runStage(measPipe.WcsVerificationStage,
            """#<?cfg paf policy?>
            sourceMatchSetKey: matchList
            """, clip)

    if stages & PHOTO_CAL:
        clip = runStage(measPipe.PhotoCalStage,
            """#<?cfg paf policy?>
            sourceMatchSetKey: matchList
            outputValueKey: photometricMagnitudeObject
            """, clip)

#        photoObj = clip['photometricMagnitudeObject']
#        if photoObj is not None:
#            print >>sys.stderr, "Photometric zero:", photoObj.getMag(1)
#            print >>sys.stderr, "Flux of a 20th mag object:", photoObj.getFlux(20)

    return clip

def run(root, visit, ccd, stages=None):
    """Run the specified visit/ccd.  If stages is omitted (or None) all available stages will be run"""
    if os.path.exists(os.path.join(root, "registry.sqlite3")):
        registry = os.path.join(root, "registry.sqlite3")
    else:
        registry = "/lsst/DC3/data/obs/CFHTLS/registry.sqlite3"
    imgCharProcess(root, ".", registry, visit=visit, ccd=ccd, stages=stages)

def main():
    cfhtMain(imgCharProcess, "calexp", "ccd")

if __name__ == "__main__":
    main()
