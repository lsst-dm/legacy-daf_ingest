#!/usr/bin/env python

import os,sys
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def isrProcess(butler, outButler, **keys):
    clip = {
        'isrExposure': butler.get("raw", **keys),
        'biasExposure': butler.get("bias", **keys),
        'darkExposure': butler.get("dark", **keys),
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
    sat = SimpleStageTester(ipPipe.SimCalibSaturationDefectStage(pol))

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

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: isrExposure
            darkexposure: darkExposure
        }
        outputKeys: {
            darkSubtractedExposure: isrExposure
        }
        """))
    dark = SimpleStageTester(ipPipe.IsrDarkStage(pol))

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
    clip = dark.runWorker(clip)
    #clip = flat.runWorker(clip)
    exposure = clip['isrExposure']
    outButler.put(exposure, "postISR", **keys)

def run():
    outvisit = 2
    if len(sys.argv) > 1:
        filter = sys.argv[1]
    else:
        filter = 'r'
    polDict = {'u':
        pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "flat_%(filter)s/imsim_0_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "../ImsimCalibRed/flat_%(filter)s/v%(visit)s/R%(raft)s/S%(sensor)s/imsim_%(visit)s_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
            cameraDescription: "../description/Full_STA_def_geom.paf"
        """)),
        'g':
        pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "flat_%(filter)s/imsim_1_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "../ImsimCalibRed/flat_%(filter)s/v%(visit)s/R%(raft)s/S%(sensor)s/imsim_%(visit)s_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
            cameraDescription: "../description/Full_STA_def_geom.paf"
        """)),
        'r':
        pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "flat_%(filter)s/imsim_2_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "../ImsimCalibRed/flat_%(filter)s/v%(visit)s/R%(raft)s/S%(sensor)s/imsim_%(visit)s_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
            cameraDescription: "../description/Full_STA_def_geom.paf"
        """)),
        'i':
        pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "flat_%(filter)s/imsim_3_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "../ImsimCalibRed/flat_%(filter)s/v%(visit)s/R%(raft)s/S%(sensor)s/imsim_%(visit)s_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
            cameraDescription: "../description/Full_STA_def_geom.paf"
        """)),
        'z':
        pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "flat_%(filter)s/imsim_4_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "../ImsimCalibRed/flat_%(filter)s/v%(visit)s/R%(raft)s/S%(sensor)s/imsim_%(visit)s_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
            cameraDescription: "../description/Full_STA_def_geom.paf"
        """)),
        'y':
        pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
            rawTemplate: "flat_%(filter)s/imsim_5_R%(raft)s_S%(sensor)s_C%(channel)s_E%(snap)03d.fits.gz"
            postISRTemplate: "../ImsimCalibRed/flat_%(filter)s/v%(visit)s/R%(raft)s/S%(sensor)s/imsim_%(visit)s_R%(raft)s_S%(sensor)s_C%(channel)s.fits"
            cameraDescription: "../description/Full_STA_def_geom.paf"
        """))
    }
    #root = os.path.join(os.environ['AFWDATA_DIR'], "imsim_tmp")
    root = "/usr/data/mysql2/ImsimCalib"
    calibRoot = "/usr/data/mysql2/ImsimCalibRed"
    bf = dafPersist.ButlerFactory(
            mapper=LsstSimMapper(
                policy=polDict[filter],
                root=root,
                calibRoot=calibRoot
            ))
    butler = bf.create()
    #obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
    #outButler = obf.create()
    for rix in range(0,5):
        for riy in range(0,5):
            if (rix, riy) not in [(0,0),(0,4),(4,0),(4,4)]:
                for six in range(0,3):
                    for siy in range(0,3):
                        for cix in range(0,2):
                            for ciy in range(0,8):
                                isrProcess(butler, butler, visit=outvisit, snap=0,
                                    raft="%i,%i"%(rix,riy), sensor="%i,%i"%(six,siy), channel="%i,%i"%(cix,ciy), filter=filter)


if __name__ == "__main__":
    run()
