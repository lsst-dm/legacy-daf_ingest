#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.meas.pipeline as measPipe
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester

def sfmProcess(root=None, outRoot=None, inButler=None, outButler=None, **keys):

    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot))
        outButler = obf.create()

    clip = {
        'scienceExposure': inButler.get("calexp", **keys),
        'psf': inButler.get("psf", **keys)
    }

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: scienceExposure
            psf: psf
        }
        outputKeys: {
            positiveDetection: positiveFootprintSet
        }
        backgroundPolicy: {
            algorithm: NONE
        }
        """))
    srcd = SimpleStageTester(measPipe.SourceDetectionStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: scienceExposure
            psf: psf
            positiveDetection: positiveFootprintSet
        }
        outputKeys: {
            sources: sourceSet
        }
        """))
    srcm = SimpleStageTester(measPipe.SourceMeasurementStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
            sources: sourceSet
            exposure: scienceExposure
        }
        """))
    skyc = SimpleStageTester(measPipe.ComputeSourceSkyCoordsStage(pol))

    clip = srcd.runWorker(clip)
    clip = srcm.runWorker(clip)
    clip = skyc.runWorker(clip)

    outButler.put(clip['sourceSet_persistable'], "src", **keys)

    fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr", 
            "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
            "IyyErr", "Ixy", "IxyErr")
    csv = open("sources-v%(visit)d-c%(ccd)d.csv" % keys, "w")
    print >>csv, "FlagForDetection," + ",".join(fields)
    for s in clip['sourceSet']:
        line = "%d" % (s.getFlagForDetection(),)
        for f in fields:
            func = getattr(s, "get" + f)
            line += ",%f" % (func(),)
        print >>csv, line
    csv.close()

def run():
    sfmProcess(root=".", outRoot=".", visit=788965, ccd=6)

if __name__ == "__main__":
    run()
