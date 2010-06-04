#!/usr/bin/env python

from lsst.datarel import lsstSimMain, lsstSimSetup, runStage

import lsst.ip.pipeline as ipPipe
import lsst.sdqa.pipeline as sdqa

def isrProcess(root=None, outRoot=None, registry=None,
        calibRoot=None, inButler=None, outButler=None, **keys):
    inButler, outButler = lsstSimSetup(root, outRoot, registry, calibRoot,
            inButler, outButler)

    clip = {
        'isrExposure': inButler.get("raw", **keys),
        'biasExposure': inButler.get("bias", **keys),
        'darkExposure': inButler.get("dark", **keys),
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

    clip = runStage(ipPipe.IsrDarkStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            darkexposure: darkExposure
        }
        outputKeys: {
            darkSubtractedExposure: isrExposure
        }
        """, clip)

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
#    outButler.put(clip['sdqaRatingVector'], "sdqaAmp", **keys)

def test():
    root = "/lsst/DC3/data/obstest/ImSim"
    if not os.path.exists("registry.sqlite3"):
        os.symlink(os.path.join(root, "registry.sqlite3"),
                "./registry.sqlite3")
    isrProcess(root=root, outRoot=".", visit=85470982, snap=0,
            raft="2,3", sensor="1,1", channel="0,0")

def main():
    lsstSimMain(isrProcess, "postISR", ("calib", "channel", "snap"),
            "/lsst/DC3/data/obstest/ImSim")

if __name__ == "__main__":
    main()
