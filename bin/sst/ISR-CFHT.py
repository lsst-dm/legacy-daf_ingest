#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def isrProcess(root, outRoot, **keys):
    bf = dafPersist.ButlerFactory(
            mapper=CfhtMapper(
                root=root,
                calibRoot="/lsst/DC3/data/obstest/CFHTLS/calib"
            ))
    butler = bf.create()
    clip = {
        'isrExposure': butler.get("raw", **keys),
        'biasExposure': butler.get("bias", **keys),
#         'darkExposure': butler.get("dark", **keys),
        'flatExposure': butler.get("flat", **keys)
    }

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            saturationCorrectedExposure: isrExposure
        }
        """))
    sat = SimpleStageTester(ipPipe.IsrSaturationStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
        }
        outputKeys: {
            overscanCorrectedExposure: isrExposure
        }
        """))
    over = SimpleStageTester(ipPipe.IsrOverscanStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            biasexposure: biasExposure
        }
        outputKeys: {
            biasSubtractedExposure: isrExposure
        }
        """))
    bias = SimpleStageTester(ipPipe.IsrBiasStage(pol))

    # pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
    #     """#<?cfg paf policy?>
    #     inputKeys: {
    #         exposure: isrExposure
    #         darkexposure: darkExposure
    #     }
    #     outputKeys: {
    #         darkSubtractedExposure: isrExposure
    #     }
    #     """))
    # dark = SimpleStageTester(ipPipe.IsrDarkStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            flatexposure: flatExposure
        }
        outputKeys: {
            flatCorrectedExposure: isrExposure
        }
        """))
    flat = SimpleStageTester(ipPipe.IsrFlatStage(pol))

    clip = sat.runWorker(clip)
    clip = over.runWorker(clip)
    clip = bias.runWorker(clip)
    # clip = dark.runWorker(clip)
    clip = flat.runWorker(clip)
    exposure = clip['isrExposure']
    # exposure.writeFits("postIsr.fits")

    # Need the input registry to get filters for output.
    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        registryPath: /lsst/DC3/data/obstest/CFHTLS/registry.sqlite3
        """))
    obf = dafPersist.ButlerFactory(mapper=CfhtMapper(pol, root=outRoot))
    outButler = obf.create()
    outButler.put(exposure, "postISR", **keys)

def run():
    root = "/lsst/DC3/data/obstest/CFHTLS"
    isrProcess(root=root, outRoot=".",
            field="D3",
            visit=788965, ccd=6, amp=0)

if __name__ == "__main__":
    run()
