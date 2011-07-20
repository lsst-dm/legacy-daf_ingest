#!/usr/bin/env python

from lsst.datarel import runStage
from lsst.pex.harness.IOStage import InputStage, OutputStage
import eups
import os
import subprocess
import sys
import time

clip = {
    'jobIdentity': {
        'visit': 85408556,
        'raft': "2,3",
        'filter': "r"
    }
}

inputDir = os.path.join("tests", "data")
clip = runStage(InputStage,
    """#<?cfg paf policy ?>
    parameters: {
        butler: {
            mapperName: lsst.obs.lsstSim.LsstSimMapper
            mapperPolicy: {
                root: """ + inputDir + """
            }
        }
        inputItems: {
            psf: {
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

assert clip.has_key('psf')
psf = clip.get('psf')
assert psf.getKernel().getHeight() == 21
assert psf.getKernel().getWidth() == 21

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
assert subprocess.call(["cmp", psfPath, os.path.join(inputDir, psfPath)]) == 0

os.remove(psfPath)
os.removedirs(psfDir)
