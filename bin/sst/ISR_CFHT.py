#!/usr/bin/env python

from lsst.datarel import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe

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

    outButler.put(clip['isrExposure'], "postISR", **keys)

def test():
    root = "/lsst/DC3/data/obstest/CFHTLS"
    isrProcess(root=root, outRoot=".", visit=788965, ccd=6, amp=0)
    isrProcess(root=root, outRoot=".", visit=788965, ccd=6, amp=1)

def main():
    cfhtMain(isrProcess, "postISR", ("calib", "amp"),
            "/lsst/DC3/data/obstest/CFHTLS")

if __name__ == "__main__":
    main()
