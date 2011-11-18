#!/usr/bin/env python

from lsst.datarel import runStage
from lsst.pex.harness.IOStage import InputStage, OutputStage
import lsst.afw.detection as afwDetection
import eups
import math
import os
import subprocess
import sys
import time

psf = afwDetection.createPsf("DoubleGaussian", 21, 21,
        5/(2*math.sqrt(2*math.log(2))), 1, 0.1)
clip = {
    'jobIdentity': {
        'visit': 85408556,
        'raft': "2,3",
        'filter': "r"
    },
    'psf': psf
}

now = time.time()
clip = runStage(OutputStage,
    """#<?cfg paf policy ?>
    parameters: {
        butler: {
            mapperName: lsst.obs.lsstSim.LsstSimMapper
            mapperPolicy: {
                root: .
            }
        }
        outputItems: {
            psf: {
                datasetId: {
                    datasetType: psf
                    fromJobIdentity: "visit" "raft" "filter"
                    set: {
                        sensor: "1,1"
                    }
                }
            }
        }
    }
    """, clip)

psfDir = os.path.join("psf", "v85408556-fr", "R23")
psfPath = os.path.join(psfDir, "S11.boost")
assert os.path.exists(psfPath)
assert os.path.getmtime(psfPath) > now

clip = runStage(InputStage,
    """#<?cfg paf policy ?>
    parameters: {
        butler: {
            mapperName: lsst.obs.lsstSim.LsstSimMapper
            mapperPolicy: {
                root: .
            }
        }
        inputItems: {
            psf2: {
                datasetType: psf
                datasetId: {
                    fromJobIdentity: "visit" "raft" "filter"
                    set: {
                        sensor: "1,1"
                    }
                }
            }
        }
    }
    """, clip)

assert clip.has_key('psf2')
psf2 = clip.get('psf2')
assert psf2.getKernel().getHeight() == psf.getKernel().getHeight()
assert psf2.getKernel().getWidth() == psf.getKernel().getWidth()
assert psf2.getKernel().getCtr() == psf.getKernel().getCtr()
assert psf2.getKernel().getNKernelParameters() == psf.getKernel().getNKernelParameters()
assert psf2.getKernel().getNSpatialParameters() == psf.getKernel().getNSpatialParameters()

now = time.time()
clip = runStage(OutputStage,
    """#<?cfg paf policy ?>
    parameters: {
        butler: {
            mapperName: lsst.obs.lsstSim.LsstSimMapper
            mapperPolicy: {
                root: .
            }
        }
        outputItems: {
            psf2: {
                datasetId: {
                    datasetType: psf
                    fromJobIdentity: "visit" "raft" "filter"
                    set: {
                        sensor: "0,0"
                    }
                }
            }
        }
    }
    """, clip)
psf2Path = os.path.join(psfDir, "S00.boost")
assert os.path.exists(psf2Path)
assert os.path.getmtime(psf2Path) > now
assert subprocess.call(["cmp", psfPath, psf2Path]) == 0

os.remove(psfPath)
os.remove(psf2Path)
os.removedirs(psfDir)
