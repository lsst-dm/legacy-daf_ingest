#!/usr/bin/env python

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
