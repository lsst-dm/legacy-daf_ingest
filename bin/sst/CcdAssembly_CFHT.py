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


from lsst.datarel import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe
import lsst.sdqa.pipeline as sdqa

def ccdAssemblyProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, None,
            inButler, outButler)

    expList = []
    for amp in (0, 1):
        expList.append(inButler.get("postISR", amp=amp, **keys))

    clip = {
        'exposureList': expList
    }

    clip = runStage(ipPipe.IsrCcdAssemblyStage,
        """#<?cfg paf policy?>
        outputKeys: {
            assembledCcdExposure: isrExposure
        }
        """, clip)
    clip = runStage(ipPipe.IsrCcdDefectStage,
        """#<?cfg paf policy?>
        inputKeys: {
            ccdExposure: isrExposure
        }
        """, clip)
    clip = runStage(ipPipe.IsrCcdSdqaStage,
        """#<?cfg paf policy?>
        inputKeys: {
            ccdExposure: isrExposure
        }
        outputKeys: {
            sdqaCcdExposure: isrExposure
        }
        """, clip)
    clip = runStage(sdqa.IsrSdqaStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposureKey: isrExposure
        }
        parameters: {
            sdqaRatingScope: 1
            sdqaMetricNames: "imageClipMean4Sig3Pass"
            sdqaMetricNames: "imageMedian"
            sdqaMetricNames: "imageSigma"
            sdqaMetricNames: "nBadCalibPix"
            sdqaMetricNames: "nSaturatePix"
            sdqaMetricNames: "imageMin"
            sdqaMetricNames: "imageMax"
        }
        outputKeys: {
            isrPersistableSdqaRatingVectorKey: sdqaRatingVector
        }
        """, clip)

    outButler.put(clip['isrExposure'], "postISRCCD", **keys)
    outButler.put(clip['sdqaRatingVector'], "sdqaCcd", **keys)

def test():
    root="."
    ccdAssemblyProcess(root=root, outRoot=".", visit=788965, ccd=0)

def main():
    cfhtMain(ccdAssemblyProcess, "postISRCCD", "ccd")

if __name__ == "__main__":
    main()
