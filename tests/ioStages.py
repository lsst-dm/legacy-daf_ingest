#!/usr/bin/env python

from lsst.datarel import runStage
from lsst.pex.harness.IOStage import InputStage, OutputStage
import os
import subprocess
import time

clip = {
    'jobIdentity': {
        'visit': 85408556,
        'raft': "2,3",
        'filter': "r"
    }
}

clip = runStage(InputStage,
    """#<?cfg paf policy ?>
    parameters: {
        butler: {
            mapperName: lsst.obs.lsstSim.LsstSimMapper
            mapperPolicy: {
                root: tests
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
assert os.path.exists("psf/v85408556-fr/R23/S11.boost")
assert os.path.getmtime("psf/v85408556-fr/R23/S11.boost") > now
assert subprocess.call(["cmp",
        "psf/v85408556-fr/R23/S11.boost",
        "tests/psf/v85408556-fr/R23/S11.boost"]) == 0

os.remove("psf/v85408556-fr/R23/S11.boost")
os.removedirs("psf/v85408556-fr/R23")
