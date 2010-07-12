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

def isrProcess(root=None, outRoot=None, registry=None,
        calibRoot=None, inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, calibRoot,
            inButler, outButler)

    clip = {
        'isrExposure': inButler.get("raw", **keys),
        'biasExposure': inButler.get("bias", **keys),
#         'darkExposure': inButler.get("dark", **keys),
        'flatExposure': inButler.get("flat", **keys)
    }

    clip = runStage(ipPipe.IsrSaturationStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            saturationMaskedExposure: isrExposure
        }
        """, clip)

    clip = runStage(ipPipe.IsrOverscanStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            overscanCorrectedExposure: isrExposure
        }
        """, clip)

    clip = runStage(ipPipe.IsrBiasStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            biasexposure: biasExposure
        }
        outputKeys: {
            biasSubtractedExposure: isrExposure
        }
        """, clip)

    clip = runStage(ipPipe.IsrVarianceStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            varianceAddedExposure: isrExposure
        }
        """, clip)

    # clip = runStage(ipPipe.IsrDarkStage,
    #     """#<?cfg paf policy?>
    #     inputKeys: {
    #         exposure: isrExposure
    #         darkexposure: darkExposure
    #     }
    #     outputKeys: {
    #         darkSubtractedExposure: isrExposure
    #     }
    #     """, clip)

    clip = runStage(ipPipe.IsrFlatStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            flatexposure: flatExposure
        }
        parameters: {
            flatScalingValue: 1.0
        }
        outputKeys: {
            flatCorrectedExposure: isrExposure
        }
        """, clip)

    clip = runStage(sdqa.IsrSdqaStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposureKey: isrExposure
        }
        parameters: {
            sdqaRatingScope: 0
            sdqaMetricNames: "overscanMean"
            sdqaMetricNames: "overscanMedian"
            sdqaMetricNames: "overscanStdDev"
            sdqaMetricNames: "overscanMin"
            sdqaMetricNames: "overscanMax"
        }
        outputKeys: {
            isrPersistableSdqaRatingVectorKey: sdqaRatingVector
        }
        """, clip)

    outButler.put(clip['isrExposure'], "postISR", **keys)
    outButler.put(clip['sdqaRatingVector'], "sdqaAmp", **keys)

def test():
    root = "/lsst/DC3/data/obstest/CFHTLS"
    isrProcess(root=root, outRoot=".", visit=788965, ccd=6, amp=0)
    isrProcess(root=root, outRoot=".", visit=788965, ccd=6, amp=1)

def main():
    cfhtMain(isrProcess, "postISR", ("calib", "amp"),
            "/lsst/DC3/data/obstest/CFHTLS")

if __name__ == "__main__":
    main()
