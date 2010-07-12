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


import sys

from lsst.datarel import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe

def crSplitProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, None,
            inButler, outButler)

    clip = {
        'isrCcdExposure': inButler.get("postISRCCD", **keys),
    }

    clip = runStage(measPipe.BackgroundEstimationStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrCcdExposure
        }
        outputKeys: {
            backgroundSubtractedExposure: bkgSubCcdExposure
        }
        parameters: {
            subtractBackground: true
            backgroundPolicy: {
                binsize: 512
            }
        }
        """, clip)
    clip = runStage(ipPipe.CrRejectStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: bkgSubCcdExposure
        }
        outputKeys: {
            exposure: crSubCcdExposure
        }
        parameters: {
            defaultFwhm: 1.0
            keepCRs: false
        }
        crRejectPolicy: {
            nCrPixelMax: 100000
        }
        """, clip)

    print >>sys.stderr, clip['nCR'], "cosmic rays"
    outButler.put(clip['crSubCcdExposure'], "visitim", **keys)

def test():
    root = "."
    crSplitProcess(root=root, outRoot=".", visit=788965, ccd=6)

def main():
    cfhtMain(crSplitProcess, "visitim", "ccd")

if __name__ == "__main__":
    main()
