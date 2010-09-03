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

from lsst.datarel import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe
import lsst.sdqa.pipeline as sdqa

def isrProcess(root=None, outRoot=None, registry=None,
        calibRoot=None, inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, calibRoot,
            inButler, outButler)

    raw = inButler.get("raw", **keys)
    bias = inButler.get("bias", **keys)
    flat = inButler.get("flat", **keys)

    clip = isrPipe(raw, bias, flat)

    outButler.put(clip['isrExposure'], "postISR", **keys)
    outButler.put(clip['sdqaRatingVector'], "sdqaAmp", **keys)

def isrPipe(raw, bias, flat):
    clip = {
        'isrExposure': raw,
        'biasExposure': bias,
        'flatExposure': flat
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

    return clip

def run(root, visit, ccd, amp):
    if os.path.exists(os.path.join(root, "registry.sqlite3")):
        registry = os.path.join(root, "registry.sqlite3")
    else:
        registry = "/lsst/DC3/data/obs/CFHTLS/registry.sqlite3"
    if os.path.exists(os.path.join(root, "calib")):
        calibRoot = os.path.join(root, "calib")
    else:
        calibRoot = "/lsst/DC3/data/obstest/CFHTLS/calib"
    isrProcess(root, ".", registry, calibRoot, visit=visit, ccd=ccd, amp=amp)

def main():
    cfhtMain(isrProcess, "postISR", ("calib", "amp"),
            "/lsst/DC3/data/obstest/CFHTLS")

if __name__ == "__main__":
    main()
